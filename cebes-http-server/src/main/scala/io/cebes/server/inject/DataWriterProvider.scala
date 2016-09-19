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
 * Created by phvu on 17/09/16.
 */

package io.cebes.server.inject

import com.google.inject.{Inject, Provider}
import io.cebes.prop.{Prop, Property}
import io.cebes.spark.storage.hdfs.HdfsDataSource
import io.cebes.storage.localfs.LocalFsDataSource
import io.cebes.storage.{DataFormat, DataWriter}

class DataWriterProvider @Inject()
(@Prop(Property.SPARK_MODE) val sparkMode: String,
 @Prop(Property.UPLOAD_PATH) val uploadPath: String) extends Provider[DataWriter] {

  override def get(): DataWriter = {
    sparkMode.toLowerCase match {
      case "local" =>
        LocalFsDataSource.ensureDirectoryExists(uploadPath)
        new LocalFsDataSource(uploadPath, DataFormat.CSV).open(false)
      case "yarn" =>
        val ds = new HdfsDataSource(uploadPath, None, DataFormat.CSV)
        ds.ensureDirectoryExists()
        ds.open(false)
      case _ => throw new IllegalArgumentException(s"Invalid spark mode: $sparkMode")
    }
  }
}

