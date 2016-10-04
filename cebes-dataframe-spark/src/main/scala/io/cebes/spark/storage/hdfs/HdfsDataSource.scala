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

package io.cebes.spark.storage.hdfs

import io.cebes.storage.DataFormats.DataFormat
import io.cebes.storage.{DataSource, DataWriter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class HdfsDataSource(val path: String,
                     val uri: Option[String],
                     val format: DataFormat) extends DataSource {

  def fullUrl: String = HdfsDataSource.getFullUrl(uri, path)

  /**
    * Open a data writer on this source, normally a file
    *
    * @param overwrite when a file exists, overwrite it if overwrite = true,
    *                  or throw an exception otherwise
    * @return a [[DataWriter]] object
    */
  override def open(overwrite: Boolean): DataWriter = {
    val fs = HdfsDataSource.getFileSystem(uri)
    val p = new Path(path)
    val fp = DataSource.validateFileName(path,
      s => fs.exists(new Path(s)),
      fs.isFile(p), fs.isDirectory(p), overwrite)

    new HdfsDataWriter(HdfsDataSource.getFullUrl(uri, fp), fs.create(new Path(fp)))
  }

  def ensureDirectoryExists(): Unit = {
    val fs = HdfsDataSource.getFileSystem(uri)
    val p = new Path(path)
    if (fs.exists(p)) {
      if (!fs.isDirectory(p)) {
        throw new IllegalArgumentException(s"Invalid directory at $path")
      }
    } else {
      if (!fs.mkdirs(p)) {
        throw new IllegalArgumentException(s"Could not create directory at $path")
      }
    }
  }
}

object HdfsDataSource {

  def getFullUrl(uri: Option[String], path: String): String = {
    val host = uri.getOrElse("localhost:9000")
    val protocolHost = if (host.startsWith("hdfs://")) {
      host
    } else {
      s"hdfs://$host"
    }
    s"$protocolHost/$path"
  }

  def getFileSystem(uri: Option[String]): FileSystem = {
    val conf = new Configuration()
    uri.foreach(conf.set("fs.defaultFS", _))
    FileSystem.get(conf)
  }
}
