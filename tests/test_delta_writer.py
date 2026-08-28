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


class RecordingDataFrameWriter:
    def __init__(self, events):
        self.events = events
        self.format_name = None
        self.mode_name = None
        self.partition_columns = None
        self.options = {}

    def format(self, value):
        self.format_name = value
        return self

    def mode(self, value):
        self.mode_name = value
        return self

    def partitionBy(self, *columns):
        self.partition_columns = columns
        return self

    def option(self, key, value):
        self.options[key] = value
        return self

    def save(self, *args, **kwargs):
        raise AssertionError("DeltaWriter must not use a path-only save")

    def saveAsTable(self, table_name):
        self.events.append(("saveAsTable", table_name))


class RecordingDataFrame:
    def __init__(self, events):
        self.write = RecordingDataFrameWriter(events)


class RecordingSpark:
    def __init__(self, events):
        self.events = events

    def sql(self, query):
        self.events.append(("sql", " ".join(query.split())))


def make_writer(**overrides):
    config = {
        "table_name": TABLE_NAME,
        "path": TABLE_PATH,
        "partition_by": [],
        "first": False,
    }
    config.update(overrides)
    events = []
    dataframe = RecordingDataFrame(events)
    writer = DeltaWriter(RecordingSpark(events), config, "2026-08-27")
    return writer, dataframe, events


class DeltaWriterTest(unittest.TestCase):
    def test_overwrite_uses_logical_table_target(self):
        writer, dataframe, events = make_writer()

        writer.overwrite(dataframe)

        self.assertEqual(dataframe.write.mode_name, "overwrite")
        self.assertIn(("saveAsTable", TABLE_NAME), events)

    def test_configured_path_is_preserved(self):
        writer, dataframe, _ = make_writer()

        writer.overwrite(dataframe)

        self.assertEqual(dataframe.write.options["path"], TABLE_PATH)

    def test_append_targets_existing_logical_table_without_recreating_it(self):
        writer, dataframe, events = make_writer(first=True)

        writer.append(dataframe)

        self.assertEqual(dataframe.write.mode_name, "append")
        self.assertIn(("saveAsTable", TABLE_NAME), events)
        self.assertFalse(any("DROP TABLE" in event[1] for event in events))
        self.assertFalse(any("CREATE TABLE" in event[1] for event in events))

    def test_partition_configuration_is_preserved(self):
        writer, dataframe, _ = make_writer(partition_by=["event_date", "region"])

        writer.overwrite_partition(dataframe)

        self.assertEqual(
            dataframe.write.partition_columns,
            ("event_date", "region"),
        )

    def test_first_drops_registration_before_table_aware_write(self):
        writer, dataframe, events = make_writer(first=True)

        writer.overwrite(dataframe)

        self.assertEqual(
            events,
            [
                ("sql", "CREATE DATABASE IF NOT EXISTS bronze"),
                ("sql", f"DROP TABLE IF EXISTS {TABLE_NAME}"),
                ("saveAsTable", TABLE_NAME),
            ],
        )

    def test_second_execution_writes_existing_table_without_drop(self):
        writer, first_dataframe, events = make_writer(first=False)
        second_dataframe = RecordingDataFrame(events)

        writer.overwrite(first_dataframe)
        writer.overwrite(second_dataframe)

        self.assertEqual(
            [event for event in events if event[0] == "saveAsTable"],
            [("saveAsTable", TABLE_NAME), ("saveAsTable", TABLE_NAME)],
        )
        self.assertFalse(any("DROP TABLE" in event[1] for event in events))


if __name__ == "__main__":
    unittest.main()
