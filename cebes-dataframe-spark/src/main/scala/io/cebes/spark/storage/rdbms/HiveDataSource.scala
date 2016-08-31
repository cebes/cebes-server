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

import io.cebes.storage.DataFormat.DataFormatEnum
import io.cebes.storage.{DataSource, DataWriter}

/**
  * An internal Hive-based data source
  *
  * @param tableName name of the table
  * @param format    Ignored. Data format doesn't play any role in this data source.
  */
class HiveDataSource(val tableName: String,
                     val format: DataFormatEnum) extends DataSource {

  /**
    * Open a data writer on this source, normally a file
    *
    * @param overwrite when a file exists, overwrite it if overwrite = true,
    *                  or throw an exception otherwise
    * @return a [[DataWriter]] object
    */
  override def open(overwrite: Boolean): DataWriter = {
    throw new UnsupportedOperationException("Opening data source is not supported in Hive")
  }
}
