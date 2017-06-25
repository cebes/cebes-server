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

import io.cebes.df.schema.Schema
import io.cebes.json.CebesCoreJsonProtocol._
import io.cebes.pipeline.json.{ModelDef, PipelineDef}
import io.cebes.spark.json.CebesSparkJsonProtocol._
import io.cebes.tag.Tag
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.util.{Failure, Success, Try}

case class TagAddRequest(tag: Tag, df: UUID)

case class TagDeleteRequest(tag: Tag)

case class TagsGetRequest(pattern: Option[String], maxCount: Int = 100)

case class TaggedDataframeResponse(tag: Tag, id: UUID, createdAt: Long, schema: Schema)

case class TaggedPipelineResponse(tag: Tag, id: UUID, createdAt: Long, pipeline: PipelineDef)

case class TaggedModelResponse(tag: Tag, id: UUID, createdAt: Long, model: ModelDef)

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
  implicit val taggedDataframeResponseFormat: RootJsonFormat[TaggedDataframeResponse] =
    jsonFormat4(TaggedDataframeResponse)
  implicit val taggedPipelineResponseFormat: RootJsonFormat[TaggedPipelineResponse] =
    jsonFormat4(TaggedPipelineResponse)
  implicit val taggedModelResponseFormat: RootJsonFormat[TaggedModelResponse] =
    jsonFormat4(TaggedModelResponse)
}

object HttpTagJsonProtocol extends HttpTagJsonProtocol