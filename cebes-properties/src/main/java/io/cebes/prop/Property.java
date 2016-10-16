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
    HIVE_METASTORE_DRIVER("CEBES_HIVE_METASTORE_DRIVER", "cebes.hive.metastore.driver", "",
            "Driver name for the hive metastore"),
    HIVE_METASTORE_USERNAME("CEBES_HIVE_METASTORE_USERNAME", "cebes.hive.metastore.username", "",
            "Username for the hive metastore"),
    HIVE_METASTORE_PASSWORD("CEBES_HIVE_METASTORE_PASSWORD", "cebes.hive.metastore.password", "",
            "Password for the hive metastore"),

    // test-only properties
    TEST_AWS_ACCESSKEY("CEBES_TEST_AWS_ACCESSKEY", "cebes.test.aws.accesskey",
            "", "AWS access key used for tests", true),
    TEST_AWS_SECRETKEY("CEBES_TEST_AWS_SECRETKEY", "cebes.test.aws.secretkey",
            "", "AWS secret key used for tests", true),

    TEST_JDBC_URL("CEBES_TEST_JDBC_URL", "cebes.test.jdbc.url",
            "", "URL for JDBC data source for tests", true),
    TEST_JDBC_USERNAME("CEBES_TEST_JDBC_USERNAME", "cebes.test.jdbc.username",
            "", "Username for JDBC data source for tests", true),
    TEST_JDBC_PASSWORD("CEBES_TEST_JDBC_PASSWORD", "cebes.test.jdbc.password",
            "", "Password for JDBC data source for tests", true);

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
