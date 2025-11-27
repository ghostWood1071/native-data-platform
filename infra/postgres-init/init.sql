DO
$$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'hive') THEN
      CREATE ROLE hive LOGIN PASSWORD 'hive';
   END IF;
END
$$;

GRANT ALL PRIVILEGES ON DATABASE metastore TO hive;


