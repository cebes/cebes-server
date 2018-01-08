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
package io.cebes.server.routes.pipeline

import io.cebes.server.routes.common.HttpTagJsonProtocol._
import io.cebes.tag.Tag
import spray.json.DefaultJsonProtocol._
import spray.json._

case class PipelineRepoLoginRequest(host: Option[String], port: Option[Int], userName: String, passwordHash: String)

case class PipelineRepoLoginResponse(host: String, port: Int, token: String)

case class PipelinePushRequest(tag: Tag, host: Option[String], port: Option[Int], token: Option[String])

trait HttpPipelineJsonProtocol {
  implicit val pipelineRepoLoginRequestFormat: RootJsonFormat[PipelineRepoLoginRequest] =
    jsonFormat4(PipelineRepoLoginRequest)
  implicit val pipelineRepoLoginResponseFormat: RootJsonFormat[PipelineRepoLoginResponse] =
    jsonFormat3(PipelineRepoLoginResponse)
  implicit val pipelinePushRequestFormat: RootJsonFormat[PipelinePushRequest] = jsonFormat4(PipelinePushRequest)
}

object HttpPipelineJsonProtocol extends HttpPipelineJsonProtocol
