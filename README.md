# cebes-server
[![Build Status](http://ec2-52-52-145-236.us-west-1.compute.amazonaws.com:8080/buildStatus/icon?job=cebes-server-pull-request)](http://ec2-52-52-145-236.us-west-1.compute.amazonaws.com:8080/job/cebes-server-pull-request)

Cebes - The integrated framework for Data Science at Scale

## Environment variables and configurations

Cebes uses something similar to [guice-property](https://github.com/phvu/guice-property) for environment variables. 
All the variables are defined in `Property.java` in the `cebes-properties` module.

## Tests

1. Configure cebes to use MySQL metastore for Hive. The relevant configurations
are:

        CEBES_HIVE_METASTORE_URL: jdbc:mysql://<host>:<port>/<database_name>
        CEBES_HIVE_METASTORE_DRIVER: com.mysql.cj.jdbc.Driver
        CEBES_HIVE_METASTORE_USERNAME: Username for the metastore database
        CEBES_HIVE_METASTORE_PASSWORD: Password
    
    You can either export those variables when running tests, or put 
    corresponding configurations in the `test/resources/application.conf` file
    of `cebes-dataframe-spark` and `cebes-http-server`.
    
    You can also use Postgres for the Hive metastore, but then you will need
    to put Postgresql jar files in the class path, since Cebes only includes
    MySQL in its jar by default.
    
2. Run the test script (with coverage report and so on):
   
        $ bin/test-all.sh

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
