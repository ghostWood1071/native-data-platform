import re
import sys
import types
import unittest


pyspark = types.ModuleType("pyspark")
pyspark_sql = types.ModuleType("pyspark.sql")
pyspark_sql.DataFrame = object
pyspark_sql.SparkSession = object
pyspark.sql = pyspark_sql
sys.modules.setdefault("pyspark", pyspark)
sys.modules.setdefault("pyspark.sql", pyspark_sql)

from src.core.writers import DeltaWriter


TABLE_NAME = "spark_catalog.bronze.olist_customers"
TABLE_PATH = "s3a://warehouse/bronze_superstore/olist_customers"


class FakeDataType:
    def __init__(self, sql_type):
        self.sql_type = sql_type

    def simpleString(self):
        return self.sql_type


class FakeField:
    def __init__(self, name, sql_type):
        self.name = name
        self.dataType = FakeDataType(sql_type)


class FakeSchema:
    def __init__(self, fields):
        self.fields = fields


class RecordingDataFrame:
    def __init__(self, events, columns=None):
        self.events = events
        self.columns = columns or ["customer_id", "customer name"]
        self.schema = FakeSchema(
            [FakeField(column, "string") for column in self.columns]
        )

    def createOrReplaceTempView(self, view_name):
        self.events.append(("createTempView", view_name))


class RecordingCatalog:
    def __init__(self, events):
        self.events = events

    def dropTempView(self, view_name):
        self.events.append(("dropTempView", view_name))


class RecordingSpark:
    def __init__(self, events, fail_insert=False):
        self.events = events
        self.fail_insert = fail_insert
        self.catalog = RecordingCatalog(events)

    def sql(self, query):
        normalized_query = " ".join(query.split())
        self.events.append(("sql", normalized_query))
        if self.fail_insert and normalized_query.startswith("INSERT "):
            raise RuntimeError("insert failed")


def make_writer(fail_insert=False, existing_delta=False, **overrides):
    config = {
        "table_name": TABLE_NAME,
        "path": TABLE_PATH,
        "partition_by": [],
        "first": False,
    }
    config.update(overrides)
    events = []
    dataframe = RecordingDataFrame(events)
    writer = DeltaWriter(
        RecordingSpark(events, fail_insert=fail_insert),
        config,
        "2026-08-27",
    )
    writer._is_delta_table = lambda path: existing_delta
    return writer, dataframe, events


def sql_events(events):
    return [event[1] for event in events if event[0] == "sql"]


class DeltaWriterTest(unittest.TestCase):
    def test_existing_delta_path_is_registered_without_explicit_schema(self):
        writer, dataframe, events = make_writer(existing_delta=True)

        writer.overwrite(dataframe)

        create_table = sql_events(events)[1]
        self.assertEqual(
            create_table,
            "CREATE TABLE IF NOT EXISTS "
            "`spark_catalog`.`bronze`.`olist_customers` "
            f"USING DELTA LOCATION '{TABLE_PATH}'",
        )

    def test_new_delta_path_is_registered_with_dataframe_schema(self):
        writer, dataframe, events = make_writer(existing_delta=False)

        writer.overwrite(dataframe)

        create_table = sql_events(events)[1]
        self.assertIn("(`customer_id` string, `customer name` string)", create_table)
        self.assertIn(f"LOCATION '{TABLE_PATH}'", create_table)

    def test_overwrite_uses_insert_overwrite_with_logical_table(self):
        writer, dataframe, events = make_writer()

        writer.overwrite(dataframe)

        insert = sql_events(events)[-1]
        self.assertIn(
            "INSERT OVERWRITE TABLE `spark_catalog`.`bronze`.`olist_customers`",
            insert,
        )
        self.assertIn(
            "(`customer_id`, `customer name`) SELECT `customer_id`, `customer name`",
            insert,
        )
        self.assertNotIn("saveAsTable", str(events))

    def test_configured_path_is_preserved_in_external_table(self):
        writer, dataframe, events = make_writer()

        writer.overwrite(dataframe)

        create_table = sql_events(events)[1]
        self.assertIn("USING DELTA", create_table)
        self.assertIn(f"LOCATION '{TABLE_PATH}'", create_table)

    def test_append_uses_insert_into_and_preserves_first_registration(self):
        writer, dataframe, events = make_writer(first=True)

        writer.append(dataframe)

        statements = sql_events(events)
        self.assertTrue(statements[-1].startswith("INSERT INTO TABLE"))
        self.assertEqual(
            statements[1],
            "DROP TABLE IF EXISTS `spark_catalog`.`bronze`.`olist_customers`",
        )
        self.assertTrue(statements[2].startswith("CREATE TABLE IF NOT EXISTS"))

    def test_partition_configuration_is_used_when_creating_table(self):
        writer, _, events = make_writer(partition_by=["event_date", "region"])
        dataframe = RecordingDataFrame(
            events,
            columns=["customer_id", "event_date", "region"],
        )

        writer.overwrite_partition(dataframe)

        create_table = sql_events(events)[1]
        self.assertIn("PARTITIONED BY (`event_date`, `region`)", create_table)

    def test_first_recreates_registration_before_insert_overwrite(self):
        writer, dataframe, events = make_writer(first=True)

        writer.overwrite(dataframe)

        statements = sql_events(events)
        self.assertEqual(
            statements[1],
            "DROP TABLE IF EXISTS `spark_catalog`.`bronze`.`olist_customers`",
        )
        self.assertTrue(statements[2].startswith("CREATE TABLE IF NOT EXISTS"))
        self.assertTrue(statements[3].startswith("INSERT OVERWRITE TABLE"))

    def test_repeated_execution_uses_unique_safe_temp_views(self):
        writer, first_dataframe, events = make_writer(first=False)
        second_dataframe = RecordingDataFrame(events)

        writer.overwrite(first_dataframe)
        writer.overwrite(second_dataframe)

        views = [event[1] for event in events if event[0] == "createTempView"]
        self.assertEqual(len(views), 2)
        self.assertNotEqual(views[0], views[1])
        self.assertTrue(
            all(re.fullmatch(r"_delta_writer_[0-9a-f]{32}", view) for view in views)
        )
        dropped = [event[1] for event in events if event[0] == "dropTempView"]
        self.assertEqual(dropped, views)

    def test_temp_view_is_dropped_when_insert_fails(self):
        writer, dataframe, events = make_writer(fail_insert=True)

        with self.assertRaisesRegex(RuntimeError, "insert failed"):
            writer.overwrite(dataframe)

        created_view = next(
            event[1] for event in events if event[0] == "createTempView"
        )
        self.assertIn(("dropTempView", created_view), events)


if __name__ == "__main__":
    unittest.main()
