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

import io.cebes.storage.DataFormat.DataFormatEnum
import io.cebes.storage.{DataSource, DataWriter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class HdfsDataSource(val path: String,
                     val uri: Option[String],
                     val format: DataFormatEnum) extends DataSource {

  def fullUrl: String = {
    val host = uri.getOrElse("localhost:9000")
    val protocolHost = if (host.startsWith("hdfs://")) {
      host
    } else {
      s"hdfs://$host"
    }
    s"$protocolHost/$path"
  }

  /**
    * Open a data writer on this source, normally a file
    *
    * @param overwrite when a file exists, overwrite it if overwrite = true,
    *                  or throw an exception otherwise
    * @return a [[DataWriter]] object
    */
  override def open(overwrite: Boolean): DataWriter = {
    val conf = new Configuration()
    if (uri.isDefined) {
      conf.set("fs.defaultFS", uri.get)
    }
    val fs = FileSystem.get(conf)
    val p = new Path(path)

    val fp = DataSource.validateFileName(path,
      s => fs.exists(new Path(s)),
      fs.isFile(p), fs.isDirectory(p), overwrite)

    new HdfsDataWriter(fs.create(new Path(fp)))
  }
}
