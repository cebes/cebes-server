#!/usr/bin/env sh

MYSQL_ROOT_PASSWORD="root_P@ssw0rd"

if [ -d /run/mysqld ]; then
    # already configure
    /usr/share/mysql/mysql.server restart --user=root
else
    mkdir /run/mysqld
    mysql_install_db --user=root

    /usr/share/mysql/mysql.server start --user=root
    echo -ne '\nn\n\n\n' | mysql_secure_installation
    mysqladmin -u root password ${MYSQL_ROOT_PASSWORD}

    TMP_FILE=`mktemp`
    if [ ! -f "${TMP_FILE}" ]; then
      return 1
    fi

    cat << EOF > ${TMP_FILE}
CREATE DATABASE cebes_hive_metastore CHARACTER SET latin1;
CREATE DATABASE cebes_store;

CREATE USER 'docker_cebes_hive'@'%' IDENTIFIED BY 'docker_cebes_hive_pwd';
CREATE USER 'docker_cebes_server'@'%' IDENTIFIED BY 'docker_cebes_server_pwd';

GRANT ALL PRIVILEGES ON cebes_hive_metastore.* TO 'docker_cebes_hive';
GRANT ALL PRIVILEGES ON cebes_store.* TO 'docker_cebes_server';
FLUSH PRIVILEGES;
EOF

    mysql --user=root -p${MYSQL_ROOT_PASSWORD} < ${TMP_FILE}
    rm -f ${TMP_FILE}
fi


