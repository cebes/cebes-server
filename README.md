# cebes-server
[![Build Status](http://cebes.io:8080/buildStatus/icon?job=cebes-server-pull-request)](http://cebes.io:8080/job/cebes-server-pull-request)

Cebes - The integrated framework for Data Science at Scale

## Environment variables and configurations

Cebes uses something similar to [guice-property](https://github.com/phvu/guice-property) for environment variables. 
All the variables are defined in `Property.java` in the `cebes-properties` module.

## Tests

1. Copy the environment variable file:

        $ cp bin/env.sh.example bin/env.sh
    
    Open the file `bin/env.sh` and set the variables, especially the username
    and password for Hive metastore, cebes store in MySQL, the test database and AWS keys:
     
        export CEBES_HIVE_METASTORE_USERNAME=""
        export CEBES_HIVE_METASTORE_PASSWORD=""
        
        export CEBES_MYSQL_USERNAME=""
        export CEBES_MYSQL_PASSWORD=""

        export CEBES_TEST_JDBC_USERNAME=""
        export CEBES_TEST_JDBC_PASSWORD=""

        export CEBES_TEST_AWS_ACCESSKEY=""
        export CEBES_TEST_AWS_SECRETKEY=""
    
    You can also use Postgres for the Hive metastore, but then you will need
    to put Postgresql jar files in the class path, since Cebes only includes
    MySQL in its jar by default.
    
2. Run the test script (with coverage report and so on):
   
        $ bin/test-all.sh
        
    Test coverage report can be exported with the `--coverage` option:
    
        $ bin/test-all.sh -c=true   # or bin/test-all.sh --coverage=true

## Logging

By default, the whole project use `scala-logging` with the `slf4j-log4j12` backend.
The configuration of `log4j` can be found in `log4j.properties` in each module of the project.

During tests, the resulting log files are normally named `${cebes.log.dir}<module_name>-test.log`.

In production, the resulting log file is named `${cebes.log.dir}cebes-http-server.log`, and rolled daily.

`Spark` has some nasty dependencies (`DataNucleus` and `parquet`), who
use either `java.util.logging` or hard-coded `log4j`. For this, we tried our best
to mute them in `cebes-http-server` with the `log4j.properties` and `parquet.logging.properties`
files.

It seems impossible to mute them in `cebes-dataframe-spark` though.

## Running Cebes in Docker

    $ sbt clean compile assembly
    $ docker build -t cebes -f docker/Dockerfile.local .
    $ docker run -it -p 21000:21000 -p 8080:8080 --name cebes-server cebes sh