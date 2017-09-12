/* Copyright 2016 The Cebes Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, version 2.0 (the "License").
 * You may not use this work except in compliance with the License,
 * which is available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 *
 * Created by phvu on 09/09/16.
 */

package io.cebes.prop;

public enum Property {

    SPARK_MODE("CEBES_SPARK_MODE", "cebes.spark.mode", "local", "Which mode to run Spark: local or yarn"),

    HTTP_INTERFACE("CEBES_HTTP_INTERFACE", "cebes.http.interface", "localhost",
            "The interface on which the HTTP service will be listening"),
    HTTP_PORT("CEBES_HTTP_PORT", "cebes.http.port", "21000",
            "The port on which the HTTP service will be listening, to be combined with HTTP_INTERFACE"),

    UPLOAD_PATH("CEBES_UPLOAD_PATH", "cebes.upload.path", "/tmp/upload/",
            "The directory to upload data to"),

    // for Hive metastore
    HIVE_METASTORE_URL("CEBES_HIVE_METASTORE_URL", "cebes.hive.metastore.url", "",
            "URL for the hive metastore"),
    HIVE_METASTORE_DRIVER("CEBES_HIVE_METASTORE_DRIVER", "cebes.hive.metastore.driver", "org.mariadb.jdbc.Driver",
            "Driver name for the hive metastore"),
    HIVE_METASTORE_USERNAME("CEBES_HIVE_METASTORE_USERNAME", "cebes.hive.metastore.username", "",
            "Username for the hive metastore"),
    HIVE_METASTORE_PASSWORD("CEBES_HIVE_METASTORE_PASSWORD", "cebes.hive.metastore.password", "",
            "Password for the hive metastore"),
    SPARK_WAREHOUSE_DIR("CEBES_SPARK_WAREHOUSE_DIR", "cebes.spark.warehouse.dir", "/tmp/spark-warehouse",
            "Parent directory to the Spark SQL warehouse"),

    CACHESPEC_DF_STORE("CEBES_CACHESPEC_DF_STORE", "cebes.cachespec.df.store",
            "maximumSize=1000,expireAfterAccess=30m",
            "Spec for the cache used for dataframe storage in cebes-spark"),

    // Non-essential properties
    CACHESPEC_RESULT_STORE("CEBES_CACHESPEC_RESULT_STORE", "cebes.cachespec.result.store",
            "maximumSize=1000,expireAfterAccess=30m",
            "Spec for the cache used for result storage in cebes-http-server"),

    // Pipeline related
    CACHESPEC_PIPELINE_STORE("CEBES_CACHESPEC_PIPELINE_STORE", "cebes.cachespec.pipeline.store",
            "maximumSize=500,expireAfterAccess=30m",
            "Spec for the cache used for pipeline storage in cebes-spark"),

    CACHESPEC_MODEL_STORE("CEBES_CACHESPEC_MODEL_STORE", "cebes.cachespec.model.store",
            "maximumSize=500,expireAfterAccess=30m",
            "Spec for the cache used for model storage in cebes-spark"),

    PIPELINE_STAGE_NAMESPACES("CEBES_PIPELINE_STAGE_NAMESPACES", "cebes.pipeline.stage.namespaces",
            "io.cebes.pipeline.models,io.cebes.pipeline.stages,io.cebes.spark.pipeline.etl," +
                    "io.cebes.spark.pipeline.features,io.cebes.spark.pipeline.ml.regression",
            "a comma-separated list of namespaces containing definition of stages"),

    MODEL_STORAGE_DIR("CEBES_MODEL_STORAGE_DIR", "cebes.model.storage.dir",
            "/tmp", "The directory to which all the models are serialized and saved"),

    // MYSQL backend
    MYSQL_URL("CEBES_MYSQL_URL", "cebes.mysql.url", "", "URL for MySQL database"),
    MYSQL_DRIVER("CEBES_MYSQL_DRIVER", "cebes.mysql.driver", "org.mariadb.jdbc.Driver", "Driver for MySQL database"),
    MYSQL_USERNAME("CEBES_MYSQL_USERNAME", "cebes.mysql.username", "", "Username for MySQL database"),
    MYSQL_PASSWORD("CEBES_MYSQL_PASSWORD", "cebes.mysql.password", "", "Password for MySQL database"),

    // test-only properties
    TEST_AWS_ACCESSKEY("CEBES_TEST_AWS_ACCESSKEY", "cebes.test.aws.accesskey",
            "", "AWS access key used for tests", true),
    TEST_AWS_SECRETKEY("CEBES_TEST_AWS_SECRETKEY", "cebes.test.aws.secretkey",
            "", "AWS secret key used for tests", true),

    TEST_JDBC_URL("CEBES_TEST_JDBC_URL", "cebes.test.jdbc.url", "",
            "URL for JDBC data source for tests", true),
    TEST_JDBC_DRIVER("CEBES_TEST_JDBC_DRIVER", "cebes.test.jdbc.driver", "org.mariadb.jdbc.Driver",
            "Driver for JDBC data source for tests", true),
    TEST_JDBC_USERNAME("CEBES_TEST_JDBC_USERNAME", "cebes.test.jdbc.username", "",
            "Username for JDBC data source for tests", true),
    TEST_JDBC_PASSWORD("CEBES_TEST_JDBC_PASSWORD", "cebes.test.jdbc.password", "",
            "Password for JDBC data source for tests", true),

    // For pipeline repository

    REPOSITORY_INTERFACE("CEBES_REPOSITORY_INTERFACE", "cebes.repository.interface", "localhost",
                           "The interface on which the HTTP service will be listening"),
    REPOSITORY_PORT("CEBES_REPOSITORY_PORT", "cebes.repository.port", "22000",
                      "The port on which the HTTP service will be listening, to be combined with REPOSITORY_INTERFACE");

    private String environmentVar;
    private String propertyKey;
    private String description;
    private String defaultValue;
    private Boolean testProperty;

    Property(String environmentVar, String propertyKey, String defaultValue,
             String description) {
        this(environmentVar, propertyKey, defaultValue, description, false);
    }

    Property(String environmentVar, String propertyKey, String defaultValue,
             String description, Boolean testOnly) {
        this.environmentVar = environmentVar;
        this.propertyKey = propertyKey;
        this.description = description;
        this.defaultValue = defaultValue;
        this.testProperty = testOnly;
    }

    public String getEnvironmentVar() {
        return environmentVar;
    }

    public String getPropertyKey() {
        return propertyKey;
    }

    public String getDescription() {
        return description;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public Boolean isTestProperty() {
        return testProperty;
    }
}
