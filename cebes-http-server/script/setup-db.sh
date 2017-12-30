#!/usr/bin/env bash

##########################################
# Script for reseting MySql Hive metastore
##########################################

CEBES_HIVE_METASTORE_DBNAME="${CEBES_HIVE_METASTORE_DBNAME:-cebes_hive_metastore}"
CEBES_HIVE_METASTORE_USERNAME="${CEBES_HIVE_METASTORE_USERNAME:-cebes_hive}"
CEBES_HIVE_METASTORE_PASSWORD="${CEBES_HIVE_METASTORE_PASSWORD:-cebes_hive_pwd}"

CEBES_MYSQL_USERNAME="${CEBES_MYSQL_USERNAME:-cebes_server}"
CEBES_MYSQL_PASSWORD="${CEBES_MYSQL_PASSWORD:-cebes_server_pwd}"

read -p "Please specify the mysql server: " MYSQL_SERVER
read -p "Please specify the mysql admin username: " MYSQL_USERNAME

curl -o hive-schema-1.2.0.mysql.sql https://raw.githubusercontent.com/apache/hive/master/metastore/scripts/upgrade/mysql/hive-schema-1.2.0.mysql.sql
curl -o hive-txn-schema-0.13.0.mysql.sql https://raw.githubusercontent.com/apache/hive/master/metastore/scripts/upgrade/mysql/hive-txn-schema-0.13.0.mysql.sql
TMP_FILE=`mktemp`

cat << EOF > ${TMP_FILE}
CREATE DATABASE IF NOT EXISTS cebes_test_db;
CREATE DATABASE IF NOT EXISTS cebes_store;

CREATE USER IF NOT EXISTS '${CEBES_MYSQL_USERNAME}' IDENTIFIED BY '${CEBES_MYSQL_PASSWORD}';

GRANT ALL PRIVILEGES ON cebes_test_db.* TO '${CEBES_MYSQL_USERNAME}';
GRANT ALL PRIVILEGES ON cebes_store.* TO '${CEBES_MYSQL_USERNAME}';

DROP DATABASE IF EXISTS ${CEBES_HIVE_METASTORE_DBNAME};
CREATE DATABASE ${CEBES_HIVE_METASTORE_DBNAME} CHARACTER SET latin1;

DROP USER IF EXISTS '${CEBES_HIVE_METASTORE_USERNAME}'@'%';
FLUSH PRIVILEGES;
CREATE USER '${CEBES_HIVE_METASTORE_USERNAME}'@'%' IDENTIFIED BY '${CEBES_HIVE_METASTORE_PASSWORD}';

GRANT ALL PRIVILEGES ON ${CEBES_HIVE_METASTORE_DBNAME}.* TO '${CEBES_HIVE_METASTORE_USERNAME}';
FLUSH PRIVILEGES;

USE ${CEBES_HIVE_METASTORE_DBNAME};
SOURCE hive-schema-1.2.0.mysql.sql;
EOF

mysql -h ${MYSQL_SERVER} -u ${MYSQL_USERNAME} -p < ${TMP_FILE}
rm -f ${TMP_FILE} hive-schema-1.2.0.mysql.sql hive-txn-schema-0.13.0.mysql.sql