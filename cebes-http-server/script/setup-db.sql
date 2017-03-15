CREATE DATABASE cebes_test_db;
CREATE DATABASE cebes_store;
CREATE DATABASE cebes_hive_metastore CHARACTER SET latin1;

CREATE USER IF NOT EXISTS 'cebes_hive' IDENTIFIED BY 'cebes_hive_pwd';
CREATE USER IF NOT EXISTS 'cebes_server' IDENTIFIED BY 'cebes_server_pwd';

GRANT ALL PRIVILEGES ON cebes_hive_metastore.* TO 'cebes_hive';
GRANT ALL PRIVILEGES ON cebes_test_db.* TO 'cebes_server';
GRANT ALL PRIVILEGES ON cebes_store.* TO 'cebes_server';