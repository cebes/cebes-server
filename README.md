# cebes-server

[![Apache 2.0 license](http://img.shields.io/badge/license-Apache_2.0-brightgreen.svg?style=flat)](https://opensource.org/licenses/Apache-2.0)
[![Build Status](http://ci.cebes.io:8080/buildStatus/icon?job=cebes-server-pull-request)](http://cebes.io:8080/job/cebes-server-pull-request)
[![Issues](http://img.shields.io/github/issues/cebes/cebes-server.svg)](https://github.com/cebes/cebes-server/issues)

[![Gitter chat](https://badges.gitter.im/cebes-io/servers.svg)](https://gitter.im/cebes-io/servers "Gitter chat")

Cebes - The integrated framework for Data Science at Scale

## Contents

* [Building](#building)
    * [Configure MariaDB](#Configure-MariaDB)
    * [Compile and run unittests](#compile-and-run-unittests)
* [Running Cebes](#Running-Cebes)
    * [Running Cebes in Docker](#Running-Cebes-in-Docker)
    * [Running Cebes locally](#Running-Cebes-locally)
    * [Running Cebes on a Spark cluster](#Running-Cebes-on-a-Spark-cluster)
* [Development guide](#development-guide)
    * [Environment variables and configurations](#environment-variables-and-configurations)
    * [Logging](#logging)
    * [Swagger documentation](#swagger-documentation)
    
## Building

To build Cebes, you need [JDK 1.8+](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html), 
[sbt](http://www.scala-sbt.org/) and a SQL database.

By default, Cebes uses [MariaDB connector](http://mariadb.org/), and should be compatible with most SQL databases.

### Configure MariaDB

This will create databases and users with default credentials for Cebes. **You only need to do this once**.
    
    mysql.server start
    ./cebes-http-server/script/setup-db.sh
    
Note that the usernames and passwords in `setup-db.sh` are default values. 
For production settings, you may want to change them to something more secured.

### Compile and run unittests

    cp bin/env.sh.example bin/env.sh
    bin/test-all.sh

Test coverage report can be exported with the `--coverage` option:
    
    bin/test-all.sh --coverage=true   # or bin/test-all.sh -c=true
    
There will be some unittests skipped. Those are tests with AWS. 
If you have an AWS account, you can enable those tests by setting the `CEBES_TEST_AWS_ACCESSKEY`
and `CEBES_TEST_AWS_SECRETKEY` variables in `bin/env.sh`.
    
The tests will run Spark in local mode.

---

## Running Cebes

You can  run Cebes in a Docker container, locally or on a Spark cluster.

### Running Cebes in Docker

Cebes can be included in a Docker image with Spark running in local mode. To build the docker image:

    sbt clean compile assembly
    docker build -t cebes -f docker/http-server/Dockerfile .
    
The docker image contains everything needed by Cebes, including a MariaDB instance. It can then be run as:

    docker run -it -p 21000:21000 -p 4040:4040 --name cebes-server cebes
    
The docker image exposes a data volume at `/cebes/data` containing Cebes logs, MariaDB databases and 
Hive warehouse used in Spark. If you want to keep the data persisted, mount it to a local directory:

    docker run -it -p 21000:21000 -p 4040:4040 -v $HOME/cebes-data:/cebes/data --name cebes-server cebes
    
To check if the Cebes server is up and running:

    curl localhost:21000/version
      {"api":"v1"}

The Spark UI can be accessed at http://localhost:4040

### Running Cebes locally

Using Docker is more preferred, but if you want you can also run Cebes with Spark in local mode:

    # start MySQL server
    mysql.server restart
    
    # compile and assembly Cebes
    sbt clean compile assembly
    
    # Download Spark and put it under ./spark
    ./bin/get-spark.sh
    
    # submit Cebes to Spark.      
    ./bin/start-cebes.sh
    
By default Cebes server will listen on port 21000 (configurable).

### Running Cebes on a Spark cluster

Use `spark-submit` script to submit the Cebes assembly jar like any other Spark application:

    sbt clean compile assembly
    CEBES_JAR=`find ./cebes-http-server/target/scala-2.11 -name cebes-http-server-assembly-*.jar | head -n 1`
    
    ${SPARK_HOME}/bin/spark-submit --class "io.cebes.server.Main" \
        --master "yarn" \
        --conf 'spark.driver.extraJavaOptions=-Dcebes.logs.dir=/tmp/' \
        ${CEBES_JAR}
        
See [Spark documentation](https://spark.apache.org/docs/latest/submitting-applications.html) for advanced options.

---

## Development guide

### Environment variables and configurations

Cebes uses something similar to [guice-property](https://github.com/phvu/guice-property) for environment variables. 
All the variables are defined in `Property.java` in the `cebes-properties` module.

### Logging

By default, the whole project use `scala-logging` with the `slf4j-log4j12` backend.
The configuration of `log4j` can be found in `log4j.properties` in each module of the project.

During tests, the resulting log files are normally named `${cebes.log.dir}<module_name>-test.log`.

In production, the resulting log file is named `${cebes.log.dir}cebes-http-server.log`, and rolled daily.

`Spark` has some nasty dependencies (`DataNucleus` and `parquet`), who
use either `java.util.logging` or hard-coded `log4j`. For this, we tried our best
to mute them in `cebes-http-server` with the `log4j.properties` and `parquet.logging.properties`
files.

It seems impossible to mute them in `cebes-spark` though.

### Swagger documentation

Projects that expose RESTful APIs (`cebes-http-server`, `cebes-pipeline-repository`, `cebes-pipeline-serving`) include
swagger definitions in `src/swagger` of respective directories. Check https://swagger.io/ for tools to generate nice-looking
UI out of that.

At the moment, some APIs might not be fixed and their swagger documentation might be not there yet. Contribute if you find
something missing!

### Get involved!

Fork the project, create an [issue](https://github.com/cebes/cebes-server/issues) 
if you find bugs or have a feature request. 

Join us on [gitter](https://gitter.im/cebes-io/servers) to interact with people. 
If you prefer the good old way, drop a message to our
[mailing list](https://groups.google.com/forum/#!forum/cebes-io).
