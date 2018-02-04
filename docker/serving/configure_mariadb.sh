#!/usr/bin/env sh

MYSQL_ROOT_PASSWORD="root_P@ssw0rd"

MARIADB_DATA_DIR=${1:-"/cebes/serving/mysql"}

if [ -f ${MARIADB_DATA_DIR}/initialized ]; then
    # already configure
    /usr/share/mysql/mysql.server start --datadir=${MARIADB_DATA_DIR} --user=root
else
    mysql_install_db --datadir=${MARIADB_DATA_DIR} --user=root
    /usr/share/mysql/mysql.server start --datadir=${MARIADB_DATA_DIR} --user=root
    echo -ne '\nn\n\n\n' | mysql_secure_installation
    mysqladmin -u root password ${MYSQL_ROOT_PASSWORD}

    TMP_FILE=`mktemp`
    if [ ! -f "${TMP_FILE}" ]; then
      return 1
    fi

    # download Hive setup scripts. Spark 2.1 ships with Hive 1.2.1
    curl -o hive-schema-1.2.0.mysql.sql https://raw.githubusercontent.com/apache/hive/master/metastore/scripts/upgrade/mysql/hive-schema-1.2.0.mysql.sql
    curl -o hive-txn-schema-0.13.0.mysql.sql https://raw.githubusercontent.com/apache/hive/master/metastore/scripts/upgrade/mysql/hive-txn-schema-0.13.0.mysql.sql

    cat << EOF > ${TMP_FILE}
CREATE DATABASE cebes_hive_metastore CHARACTER SET latin1;
CREATE DATABASE cebes_store;

CREATE USER 'docker_cebes_hive'@'%' IDENTIFIED BY 'docker_cebes_hive_pwd';
CREATE USER 'docker_cebes_server'@'%' IDENTIFIED BY 'docker_cebes_server_pwd';

GRANT ALL PRIVILEGES ON cebes_hive_metastore.* TO 'docker_cebes_hive';
GRANT ALL PRIVILEGES ON cebes_store.* TO 'docker_cebes_server';
FLUSH PRIVILEGES;

USE cebes_hive_metastore;
SOURCE hive-schema-1.2.0.mysql.sql;
EOF

    mysql --user=root -p${MYSQL_ROOT_PASSWORD} < ${TMP_FILE}
    rm -f ${TMP_FILE} hive-schema-1.2.0.mysql.sql hive-txn-schema-0.13.0.mysql.sql

    touch ${MARIADB_DATA_DIR}/initialized
fi


