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

import akka.actor.ActorSystem
import akka.http.scaladsl.server.Directives.{handleExceptions, _}
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import com.google.inject.{Inject, Injector}
import io.cebes.auth.AuthService
import io.cebes.http.server.HttpServer
import io.cebes.http.server.auth.AuthHandler
import io.cebes.http.server.routes.result.ResultHandler
import io.cebes.http.server.routes.{ApiErrorHandler, SecuredSession}
import io.cebes.pipeline.InferenceService
import io.cebes.pipeline.json.ServingConfiguration
import io.cebes.prop.{Prop, Property}
import io.cebes.serving.common.ServingActor
import io.cebes.serving.routes.InferenceHandler

import scala.concurrent.ExecutionContext

class CebesServingSecuredServer @Inject()(private val servingConfiguration: ServingConfiguration,
                                          private val servingActor: ServingActor,
                                          @Prop(Property.SERVING_SERVER_SECRET)
                                          override protected val serverSecret: String,
                                          override protected val authService: AuthService,
                                          override protected val inferenceService: InferenceService,
                                          override protected val refreshTokenStorage: CebesServingRefreshTokenStorage,
                                          override protected val injector: Injector)
  extends HttpServer with AuthHandler with InferenceHandler
    with SecuredSession with ApiErrorHandler with ResultHandler {

  override val httpInterface: String = servingConfiguration.httpInterface
  override val httpPort: Int = servingConfiguration.httpPort

  override protected implicit def actorSystem: ActorSystem = servingActor.actorSystem

  override protected implicit def actorExecutor: ExecutionContext = servingActor.actorExecutor

  override protected implicit def actorMaterializer: Materializer = servingActor.actorMaterializer

  override val routes: Route = handleExceptions(cebesDefaultExceptionHandler) {
    authApi ~
      requiredCebesSession { _ =>
        inferenceApi ~ resultApi
      }
  }

}
