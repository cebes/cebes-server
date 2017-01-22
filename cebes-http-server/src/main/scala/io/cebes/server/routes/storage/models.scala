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
 * Created by phvu on 18/12/2016.
 */

package io.cebes.server.routes.storage

import io.cebes.server.routes.HttpJsonProtocol
import io.cebes.storage.DataFormats.DataFormat

case class LocalFsReadRequest(path: String, format: DataFormat)

case class S3ReadRequest(accessKey: String, secretKey: String,
                         regionName: Option[String],
                         bucketName: String, key: String,
                         format: DataFormat)

case class HdfsReadRequest(path: String, uri: Option[String], format: DataFormat)

case class JdbcReadRequest(url: String, tableName: String,
                           userName: String, passwordBase64: String,
                           driver: Option[String])

case class HiveReadRequest(tableName: String)

case class ReadRequest(localFs: Option[LocalFsReadRequest],
                       s3: Option[S3ReadRequest],
                       hdfs: Option[HdfsReadRequest],
                       jdbc: Option[JdbcReadRequest],
                       hive: Option[HiveReadRequest])

case class UploadResponse(path: String, size: Int)

trait HttpStorageJsonProtocol extends HttpJsonProtocol {

  implicit val localFsReadRequestFormat = jsonFormat2(LocalFsReadRequest)
  implicit val s3ReadRequestFormat = jsonFormat6(S3ReadRequest)
  implicit val hdfsReadRequestFormat = jsonFormat3(HdfsReadRequest)
  implicit val jdbcReadRequestFormat = jsonFormat5(JdbcReadRequest)
  implicit val hiveReadRequestFormat = jsonFormat1(HiveReadRequest)
  implicit val readRequestFormat = jsonFormat5(ReadRequest)

  implicit val uploadResponseFormat = jsonFormat2(UploadResponse)

}

object HttpStorageJsonProtocol extends HttpStorageJsonProtocol