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

package io.cebes.server.routes

import java.util.UUID

import io.cebes.df.schema.Schema
import io.cebes.json.CebesCoreJsonProtocol
import spray.json._

object RequestStatuses {

  sealed abstract class RequestStatus(val name: String)

  case object SCHEDULED extends RequestStatus("scheduled")

  case object FINISHED extends RequestStatus("finished")

  case object FAILED extends RequestStatus("failed")

  val values = Seq(SCHEDULED, FINISHED, FAILED)

  def fromString(name: String): Option[RequestStatus] = values.find(_.name == name)
}

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

case class VersionResponse(api: String)

/** **************************************************************************/
// Contains all common JsonProtocol
/** **************************************************************************/

trait HttpJsonProtocol extends CebesCoreJsonProtocol {

  implicit object RequestStatusFormat extends JsonFormat[RequestStatuses.RequestStatus] {
    override def write(obj: RequestStatuses.RequestStatus): JsValue = JsString(obj.name)

    override def read(json: JsValue): RequestStatuses.RequestStatus = json match {
      case JsString(fmtName) => RequestStatuses.fromString(fmtName) match {
        case Some(fmt) => fmt
        case None => deserializationError(s"Unrecognized Request Status: $fmtName")
      }
      case _ => deserializationError(s"Expected RequestStatus as a string")
    }
  }

  implicit val futureResultFormat = jsonFormat1(FutureResult)

  implicit val serializableResultFormat = jsonFormat4(SerializableResult)

  implicit val failResponseFormat = jsonFormat2(FailResponse)
  implicit val okResponseFormat = jsonFormat1(OkResponse)
  implicit val dataframeResponseFormat = jsonFormat2(DataframeResponse)
  implicit val versionResponseFormat = jsonFormat1(VersionResponse)
}

object HttpJsonProtocol extends HttpJsonProtocol
