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
 * Created by phvu on 31/08/16.
 */

package io.cebes.spark.storage.rdbms

import java.util.Properties

import io.cebes.storage.DataFormats.DataFormat
import io.cebes.storage.{DataFormats, DataSource, DataWriter}

/**
  * A JDBC data source
  *
  * @param url            JDBC URL
  * @param tableName      name of the table
  * @param userName       user name
  * @param password password, encoded with base64 (with UTF-8 charset)
  */
class JdbcDataSource(val url: String,
                     val tableName: String,
                     val userName: String,
                     val password: String,
                     val driver: Option[String]) extends DataSource {

  /**
    * Ignored. Data format doesn't play any role in this data source.
    */
  override val format: DataFormat = DataFormats.UNKNOWN

  def sparkProperties() = {
    val prop = new Properties()
    prop.setProperty("user", userName)
    prop.setProperty("password", password)
    driver.foreach(prop.setProperty("driver", _))
    prop
  }

  /**
    * Open a data writer on this source, normally a file
    *
    * @param overwrite when a file exists, overwrite it if overwrite = true,
    *                  or throw an exception otherwise
    * @return a [[DataWriter]] object
    */
  override def open(overwrite: Boolean): DataWriter = {
    throw new UnsupportedOperationException("Opening data source is not supported in JDBC")
  }
}
