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
package io.cebes.serving.http

import javax.inject.Inject

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import io.cebes.http.server.HttpServer
import io.cebes.serving.CebesServingJsonProtocol._
import io.cebes.serving.common.ServingActor
import io.cebes.serving.{InferenceRequest, PipelineServingService, ServingConfiguration}

import scala.concurrent.ExecutionContextExecutor

class CebesServingServer @Inject()(private val servingConfiguration: ServingConfiguration,
                                   private val servingService: PipelineServingService,
                                   private val servingActor: ServingActor) extends HttpServer {

  override val httpInterface: String = servingConfiguration.httpInterface
  override val httpPort: Int = servingConfiguration.httpPort

  protected implicit val actorSystem: ActorSystem = servingActor.actorSystem
  protected implicit val actorExecutor: ExecutionContextExecutor = servingActor.actorExecutor
  protected implicit val actorMaterializer: ActorMaterializer = servingActor.actorMaterializer

  override protected val routes: Route =
    (path("inference") & post) {
      entity(as[InferenceRequest]) { request =>
        complete(servingService.inference(request))
      }
    }
}
