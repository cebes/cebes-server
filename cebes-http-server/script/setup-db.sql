CREATE DATABASE cebes_test_db;
CREATE DATABASE cebes_store;
CREATE DATABASE cebes_hive_metastore;

GRANT ALL PRIVILEGES ON cebes_hive_metastore.* TO 'cebes_hive' IDENTIFIED BY 'cebes_hive_pwd';
GRANT ALL PRIVILEGES ON cebes_test_db.* TO 'cebes_server' IDENTIFIED BY 'cebes_server_pwd';
GRANT ALL PRIVILEGES ON cebes_store.* TO 'cebes_server';