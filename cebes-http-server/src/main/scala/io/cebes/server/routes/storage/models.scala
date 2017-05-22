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

import io.cebes.json.CebesCoreJsonProtocol._
import io.cebes.storage.DataFormats.DataFormat
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

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

trait HttpStorageJsonProtocol {

  implicit val localFsReadRequestFormat: RootJsonFormat[LocalFsReadRequest] = jsonFormat2(LocalFsReadRequest)
  implicit val s3ReadRequestFormat: RootJsonFormat[S3ReadRequest] = jsonFormat6(S3ReadRequest)
  implicit val hdfsReadRequestFormat: RootJsonFormat[HdfsReadRequest] = jsonFormat3(HdfsReadRequest)
  implicit val jdbcReadRequestFormat: RootJsonFormat[JdbcReadRequest] = jsonFormat5(JdbcReadRequest)
  implicit val hiveReadRequestFormat: RootJsonFormat[HiveReadRequest] = jsonFormat1(HiveReadRequest)
  implicit val readRequestFormat: RootJsonFormat[ReadRequest] = jsonFormat5(ReadRequest)

  implicit val uploadResponseFormat: RootJsonFormat[UploadResponse] = jsonFormat2(UploadResponse)

}

object HttpStorageJsonProtocol extends HttpStorageJsonProtocol
