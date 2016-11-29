CREATE DATABASE cebes_test_db;
CREATE DATABASE cebes_store;
CREATE DATABASE hive_metastore;

GRANT ALL PRIVILEGES ON hive_metastore.* TO 'cebes_hive' IDENTIFIED BY 'cebes_hive_pwd';
GRANT ALL PRIVILEGES ON cebes_test_db.* TO 'cebes' IDENTIFIED BY 'cebespwd';
GRANT ALL PRIVILEGES ON cebes_store.* TO 'cebes';