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

import io.cebes.df.schema.{Schema, SchemaJsonProtocol}
import io.cebes.server.models.RequestStatuses.RequestStatus
import io.cebes.storage.DataFormats
import io.cebes.storage.DataFormats.DataFormat
import spray.json.{DefaultJsonProtocol, JsString, JsValue, JsonFormat, deserializationError}

package object models {

  object RequestStatuses {
    sealed abstract class RequestStatus(val name: String)

    case object SCHEDULED extends RequestStatus("scheduled")
    case object FINISHED extends RequestStatus("finished")
    case object FAILED extends RequestStatus("failed")

    val values = Seq(SCHEDULED, FINISHED, FAILED)

    def fromString(name: String): Option[RequestStatus] = values.find(_.name == name)
  }

  // Objects in requests
  case class UserLogin(userName: String, passwordHash: String)

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

  /** **************************************************************************/

  // Future response
  case class FutureResult(requestId: UUID)

  case class SerializableResult(requestId: UUID,
                                status: RequestStatuses.RequestStatus,
                                response: Option[JsValue],
                                request: Option[JsValue])

  /** **************************************************************************/

  // a request was failed for whatever reason
  case class FailResponse(message: Option[String], stackTrace: Option[String])

  // Results of synchronous commands will belong to following classes
  case class OkResponse(message: String)

  case class DataframeResponse(id: UUID, schema: Schema)

  case class UploadResponse(path: String, size: Int)

  /** **************************************************************************/
  // Contains all JsonProtocol for Cebes HTTP server
  /** **************************************************************************/

  trait CebesJsonProtocol extends DefaultJsonProtocol with SchemaJsonProtocol {

    // clumsy custom JsonFormats
    implicit object UUIDFormat extends JsonFormat[UUID] {

      def write(obj: UUID): JsValue = JsString(obj.toString)

      def read(json: JsValue): UUID = json match {
        case JsString(x) => UUID.fromString(x)
        case _ => deserializationError("Expected UUID as JsString")
      }
    }

    implicit object DataFormatFormat extends JsonFormat[DataFormat] {
      override def write(obj: DataFormat): JsValue = JsString(obj.name)

      override def read(json: JsValue): DataFormat = json match {
        case JsString(fmtName) => DataFormats.fromString(fmtName) match {
          case Some(fmt) => fmt
          case None => deserializationError(s"Unrecognized Data format: $fmtName")
        }
        case _ => deserializationError(s"Expected DataFormat as a string")
      }
    }

    implicit object RequestStatusFormat extends JsonFormat[RequestStatus] {
      override def write(obj: RequestStatus): JsValue = JsString(obj.name)

      override def read(json: JsValue): RequestStatus = json match {
        case JsString(fmtName) => RequestStatuses.fromString(fmtName) match {
          case Some(fmt) => fmt
          case None => deserializationError(s"Unrecognized Request Status: $fmtName")
        }
        case _ => deserializationError(s"Expected RequestStatus as a string")
      }
    }

    implicit val userLoginFormat = jsonFormat2(UserLogin)

    implicit val localFsReadRequestFormat = jsonFormat2(LocalFsReadRequest)
    implicit val s3ReadRequestFormat = jsonFormat6(S3ReadRequest)
    implicit val hdfsReadRequestFormat = jsonFormat3(HdfsReadRequest)
    implicit val jdbcReadRequestFormat = jsonFormat5(JdbcReadRequest)
    implicit val hiveReadRequestFormat = jsonFormat1(HiveReadRequest)
    implicit val readRequestFormat = jsonFormat5(ReadRequest)

    implicit val futureResultFormat = jsonFormat1(FutureResult)

    implicit val serializableResultFormat = jsonFormat4(SerializableResult)

    //

    implicit val failResponseFormat = jsonFormat2(FailResponse)
    implicit val okResponseFormat = jsonFormat1(OkResponse)
    implicit val dataframeResponseFormat = jsonFormat2(DataframeResponse)
    implicit val uploadResponseFormat = jsonFormat2(UploadResponse)
  }

  object CebesJsonProtocol extends CebesJsonProtocol

}
