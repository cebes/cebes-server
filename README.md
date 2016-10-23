# cebes-server
[![Build Status](http://ec2-52-53-151-47.us-west-1.compute.amazonaws.com:8080/buildStatus/icon?job=cebes-server-pull-request)](http://ec2-52-53-151-47.us-west-1.compute.amazonaws.com:8080/job/cebes-server-pull-request)

Cebes - The Data Scientist's toolbox for Big Data

## Environment variables and configurations

Cebes uses something similar to [guice-property](https://github.com/phvu/guice-property) for environment variables. 
All the variables are defined in `Property.java` in the `cebes-properties` module.

## Tests

Since both `cebes-dataframe-spark` and `cebes-http-server` uses Spark
with Hive enabled, by default both modules uses Hive with Derby metastore.

When you run `sbt test` at the root project, both modules will use the 
same Derby metastore, which then cause troubles, the tests won't be 
able to finish.

There are two ways to overcome this (until we figure out a proper solution):

1. Configure cebes to use PostgreSQL for tests. The relevant configurations
are:

        CEBES_HIVE_METASTORE_URL: jdbc:postgresql://<host>:<port>/<database_name>
        CEBES_HIVE_METASTORE_DRIVER: org.postgresql.Driver
        CEBES_HIVE_METASTORE_USERNAME: Username for the metastore database
        CEBES_HIVE_METASTORE_PASSWORD: Password
    
    You can either export those variables when running tests, or put 
    corresponding configurations in the `test/resources/application.conf`
    of `cebes-dataframe-spark` and `cebes-http-server`.
    
    You can also use MySQL for the Hive metastore, but then you will need
    to put MySQL jar files in the class path, since Cebes only includes
    PostgreSQL in its jar by default.
    
    See [this](http://www.cloudera.com/documentation/archive/cdh/4-x/4-2-1/CDH4-Installation-Guide/cdh4ig_topic_18_4.html) to know how to use MySQL for Hive metastore.
    

