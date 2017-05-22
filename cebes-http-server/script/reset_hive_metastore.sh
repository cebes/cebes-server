#!/usr/bin/env bash

##########################################
# Script for reseting MySql Hive metastore
##########################################

curl -o hive-schema-1.2.0.mysql.sql https://raw.githubusercontent.com/apache/hive/master/metastore/scripts/upgrade/mysql/hive-schema-1.2.0.mysql.sql
curl -o hive-txn-schema-0.13.0.mysql.sql https://raw.githubusercontent.com/apache/hive/master/metastore/scripts/upgrade/mysql/hive-txn-schema-0.13.0.mysql.sql
TMP_FILE=`mktemp`

cat << EOF > ${TMP_FILE}
USE cebes_hive_metastore;
SOURCE hive-schema-1.2.0.mysql.sql;
EOF

mysql --user=root -p < ${TMP_FILE}
rm -f ${TMP_FILE} hive-schema-1.2.0.mysql.sql hive-txn-schema-0.13.0.mysql.sql