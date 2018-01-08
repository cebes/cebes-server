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
 */
package io.cebes.repository.client

import akka.http.scaladsl.model.HttpHeader
import spray.json.DefaultJsonProtocol._
import spray.json._

trait HttpHeaderJsonProtocol {

  implicit object HttpHeaderJsonFormat extends JsonFormat[HttpHeader] {
    override def write(obj: HttpHeader): JsValue =
      JsObject(Map("name" -> obj.name().toJson, "value" -> obj.value().toJson))

    override def read(json: JsValue): HttpHeader = json match {
      case jsObj: JsObject =>
        require(jsObj.fields.contains("name") && jsObj.fields.contains("value"),
          "JSON object must have a name and a value")
        HttpHeader.parse(jsObj.fields("name").convertTo[String], jsObj.fields("value").convertTo[String]) match {
          case HttpHeader.ParsingResult.Ok(header, Nil) =>
            header
          case _ =>
            deserializationError("Unable to deserialize Json object")
        }
      case other =>
        deserializationError(s"Required a JSON object, get ${other.toString()}")
    }
  }

}

object HttpHeaderJsonProtocol extends HttpHeaderJsonProtocol
