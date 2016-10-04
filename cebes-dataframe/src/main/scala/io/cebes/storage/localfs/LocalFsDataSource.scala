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
 * Created by phvu on 30/08/16.
 */

package io.cebes.storage.localfs

import java.io.File

import io.cebes.storage.DataFormats.DataFormat
import io.cebes.storage.{DataSource, DataWriter}


class LocalFsDataSource(val path: String, val format: DataFormat) extends DataSource {

  /**
    * Open a data writer on this source, normally a file
    *
    * @param overwrite when a file exists, overwrite it if overwrite = true,
    *                  or throw an exception otherwise
    * @return a [[DataWriter]] object
    */
  override def open(overwrite: Boolean): DataWriter = {
    val f = new File(path)
    val fp = DataSource.validateFileName(path,
      x => new File(x).exists(),
      f.isFile, f.isDirectory, overwrite)

    new LocalFsDataWriter(fp)
  }
}

object LocalFsDataSource {

  def ensureDirectoryExists(path: String): Unit = {
    val f = new File(path)
    if (f.exists()) {
      if (!f.isDirectory) {
        throw new IllegalArgumentException(s"Invalid directory at $path")
      }
    } else {
      if (!f.mkdirs()) {
        throw new IllegalArgumentException(s"Could not create directory at $path")
      }
    }
  }
}
