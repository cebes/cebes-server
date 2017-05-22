/* Copyright 2017 The Cebes Authors. All Rights Reserved.
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
package io.cebes.server.routes.common

import java.util.UUID

import io.cebes.json.CebesCoreJsonProtocol._
import io.cebes.tag.Tag
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.util.{Failure, Success, Try}

case class TagAddRequest(tag: Tag, df: UUID)

case class TagDeleteRequest(tag: Tag)

case class TagsGetRequest(pattern: Option[String], maxCount: Int = 100)

trait HttpTagJsonProtocol {

  implicit object TagFormat extends JsonFormat[Tag] {
    override def write(obj: Tag): JsValue = JsString(obj.toString)

    override def read(json: JsValue): Tag = json match {
      case jsStr: JsString =>
        Try(Tag.fromString(jsStr.value)) match {
          case Success(t) => t
          case Failure(f) => deserializationError(s"Failed to parse tag: ${f.getMessage}", f)
        }
      case other =>
        deserializationError(s"Expected a JsString, got ${other.compactPrint}")
    }
  }

  implicit val tagAddRequestFormat: RootJsonFormat[TagAddRequest] = jsonFormat2(TagAddRequest)
  implicit val tagDeleteRequestFormat: RootJsonFormat[TagDeleteRequest] = jsonFormat1(TagDeleteRequest)
  implicit val tagsGetRequestFormat: RootJsonFormat[TagsGetRequest] = jsonFormat2(TagsGetRequest)
}

object HttpTagJsonProtocol extends HttpTagJsonProtocol