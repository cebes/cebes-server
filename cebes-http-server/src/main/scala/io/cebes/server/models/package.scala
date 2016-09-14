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

import spray.json.{DefaultJsonProtocol, JsString, JsValue, JsonFormat, deserializationError}

package object models {

  // Objects in requests
  case class UserLogin(userName: String, passwordHash: String)

  /** **************************************************************************/

  // objects in responses

  // Future response
  case class FutureResult(requestId: UUID)

  /** **************************************************************************/

  case class Request[T](entity: T, uri: String)

  // Results of synchronous commands will belong to following classes
  case class OkResponse(message: String)

  // when user asks for results of a particular request
  case class Result[T, R](request: Request[T], response: R)

  /** **************************************************************************/
  // Contains all JsonProtocol for Cebes HTTP server
  /** **************************************************************************/

  trait CebesJsonProtocol extends DefaultJsonProtocol {

    implicit val userLoginFormat = jsonFormat2(UserLogin)

    // clumsy custom JsonFormats
    implicit object UUIDFormat extends JsonFormat[UUID] {

      def write(obj: UUID): JsValue = JsString(obj.toString)

      def read(json: JsValue): UUID = json match {
        case JsString(x) => UUID.fromString(x)
        case _ => deserializationError("Expected UUID as JsString")
      }
    }

    implicit val futureResultFormat = jsonFormat1(FutureResult)

    implicit def requestFormat[T](implicit jf: JsonFormat[T]) = jsonFormat2(Request[T])

    implicit val okResponseFormat = jsonFormat1(OkResponse)

    implicit def resultFormat[T, R](implicit jf1: JsonFormat[T], jf2: JsonFormat[R]) = jsonFormat2(Result[T, R])
  }

  object CebesJsonProtocol extends CebesJsonProtocol

}
