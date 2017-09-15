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
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import io.cebes.http.server.HttpServer
import io.cebes.prop.{Prop, Property}

import scala.concurrent.ExecutionContextExecutor

class CebesServingServer @Inject()(@Prop(Property.REPOSITORY_INTERFACE) override val httpInterface: String,
                                   @Prop(Property.REPOSITORY_PORT) override val httpPort: Int) extends HttpServer {

  protected implicit val actorSystem: ActorSystem = ActorSystem("CebesPipelineServing")

  protected implicit val actorExecutor: ExecutionContextExecutor = actorSystem.dispatcher
  protected implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()

  override protected val routes: Route = _
}
