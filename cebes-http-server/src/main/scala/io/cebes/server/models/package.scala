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

package io.cebes.server

import java.util.UUID

import io.cebes.storage.DataFormat
import io.cebes.storage.DataFormat.DataFormatEnum
import spray.json.{DefaultJsonProtocol, JsString, JsValue, JsonFormat, deserializationError}

package object models {

  // Objects in requests
  case class UserLogin(userName: String, passwordHash: String)

  case class LocalFsReadRequest(path: String, format: DataFormatEnum)

  case class S3ReadRequest(accessKey: String, secretKey: String,
                           bucketName: String, key: String,
                           format: DataFormatEnum)

  case class HdfsReadRequest(path: String, uri: Option[String], format: DataFormatEnum)

  case class JdbcReadRequest(url: String, tableName: String,
                             userName: String, passwordBase64: String)

  case class HiveReadRequest(tableName: String)

  case class ReadRequest(localFs: Option[LocalFsReadRequest],
                         s3: Option[S3ReadRequest],
                         hdfs: Option[HdfsReadRequest],
                         jdbc: Option[JdbcReadRequest],
                         hive: Option[HiveReadRequest])

  /** **************************************************************************/

  // Future response
  case class FutureResult(requestId: UUID)

  case class Request[E](entity: E, uri: String, requestId: UUID)

  // when user asks for results of a particular request
  case class Result[E, R](request: Request[E], response: R)

  /** **************************************************************************/

  // a request was failed for whatever reason
  case class FailResponse(message: String, stackTrace: String)

  // Results of synchronous commands will belong to following classes
  case class OkResponse(message: String)

  case class DataframeResponse(id: UUID)

  case class UploadResponse(path: String, size: Int)

  /** **************************************************************************/
  // Contains all JsonProtocol for Cebes HTTP server
  /** **************************************************************************/

  trait CebesJsonProtocol extends DefaultJsonProtocol {

    // clumsy custom JsonFormats
    implicit object UUIDFormat extends JsonFormat[UUID] {

      def write(obj: UUID): JsValue = JsString(obj.toString)

      def read(json: JsValue): UUID = json match {
        case JsString(x) => UUID.fromString(x)
        case _ => deserializationError("Expected UUID as JsString")
      }
    }

    implicit object DataFormatEnumFormat extends JsonFormat[DataFormatEnum] {
      override def write(obj: DataFormatEnum): JsValue = JsString(obj.name)

      override def read(json: JsValue): DataFormatEnum = json match {
        case JsString(fmtName) => DataFormat.fromString(fmtName) match {
          case Some(fmt) => fmt
          case None => deserializationError(s"Unrecognized Data format: $fmtName")
        }
        case _ => deserializationError(s"Expected DataFormat as a string")
      }
    }

    implicit val userLoginFormat = jsonFormat2(UserLogin)

    implicit val localFsReadRequestFormat = jsonFormat2(LocalFsReadRequest)
    implicit val s3ReadRequestFormat = jsonFormat5(S3ReadRequest)
    implicit val hdfsReadRequestFormat = jsonFormat3(HdfsReadRequest)
    implicit val jdbcReadRequestFormat = jsonFormat4(JdbcReadRequest)
    implicit val hiveReadRequestFormat = jsonFormat1(HiveReadRequest)
    implicit val readRequestFormat = jsonFormat5(ReadRequest)

    implicit val futureResultFormat = jsonFormat1(FutureResult)

    implicit def requestFormat[T](implicit jf: JsonFormat[T]) = jsonFormat3(Request[T])

    implicit def resultFormat[T, R](implicit jf1: JsonFormat[T], jf2: JsonFormat[R]) = jsonFormat2(Result[T, R])

    //

    implicit val failResponseFormat = jsonFormat2(FailResponse)
    implicit val okResponseFormat = jsonFormat1(OkResponse)
    implicit val dataframeResponseFormat = jsonFormat1(DataframeResponse)
    implicit val uploadResponseFormat = jsonFormat2(UploadResponse)
  }

  object CebesJsonProtocol extends CebesJsonProtocol

}
