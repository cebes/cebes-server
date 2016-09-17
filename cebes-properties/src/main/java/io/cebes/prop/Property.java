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
            "The directory to upload data to");

    private String environmentVar;
    private String propertyKey;
    private String description;
    private String defaultValue;

    Property(String environmentVar, String propertyKey, String defaultValue, String description) {
        this.environmentVar = environmentVar;
        this.propertyKey = propertyKey;
        this.description = description;
        this.defaultValue = defaultValue;
    }

    public String getDescription() {
        return description;
    }

    public String getValue() {
        return System.getenv().getOrDefault(this.environmentVar,
                System.getProperty(this.propertyKey, this.defaultValue));
    }

}
