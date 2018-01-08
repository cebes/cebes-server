#!/usr/bin/env sh

MYSQL_ROOT_PASSWORD="root_P@ssw0rd"

MARIADB_DATA_DIR=${1:-"/cebes/data/mysql"}

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

    cat << EOF > ${TMP_FILE}
CREATE DATABASE cebes_store;

CREATE USER 'docker_cebes_server'@'%' IDENTIFIED BY 'docker_cebes_server_pwd';

GRANT ALL PRIVILEGES ON cebes_store.* TO 'docker_cebes_server';
FLUSH PRIVILEGES;

EOF

    mysql --user=root -p${MYSQL_ROOT_PASSWORD} < ${TMP_FILE}
    rm -f ${TMP_FILE}

    touch ${MARIADB_DATA_DIR}/initialized
fi


