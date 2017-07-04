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
package io.cebes.server.routes.pipeline

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.cebes.pipeline.json._
import io.cebes.server.http.SecuredSession
import io.cebes.server.routes.common.HttpTagJsonProtocol._
import io.cebes.server.routes.common._
import io.cebes.spark.json.CebesSparkJsonProtocol._
import spray.json.DefaultJsonProtocol.{StringJsonFormat, arrayFormat}

trait PipelineHandler extends SecuredSession with OperationHelper with LazyLogging {

  val pipelineApi: Route = pathPrefix("pipeline") {
    myRequiredSession { _ =>
      concat(
        operation[TagAdd, TagAddRequest, PipelineDef],
        operation[TagDelete, TagDeleteRequest, PipelineDef],
        operation[Tags, TagsGetRequest, Array[TaggedPipelineResponse]],
        operation[Get, String, PipelineDef],

        operation[Create, PipelineDef, PipelineDef],
        operation[Run, PipelineRunDef, PipelineRunResultDef]
      )
    }
  }
}