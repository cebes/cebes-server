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
package io.cebes.server.routes.common

import java.util.UUID

import io.cebes.df.schema.Schema
import io.cebes.json.CebesCoreJsonProtocol._
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

case class DataframeResponse(id: UUID, schema: Schema)

case class VersionResponse(api: String)

trait HttpServerJsonProtocol {
  implicit val dataframeResponseFormat: RootJsonFormat[DataframeResponse] = jsonFormat2(DataframeResponse)
  implicit val versionResponseFormat: RootJsonFormat[VersionResponse] = jsonFormat1(VersionResponse)
}

object HttpServerJsonProtocol extends HttpServerJsonProtocol