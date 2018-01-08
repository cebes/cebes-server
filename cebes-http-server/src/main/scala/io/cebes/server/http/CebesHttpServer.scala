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
package io.cebes.server.http

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import com.google.inject.{Inject, Injector}
import io.cebes.auth.AuthService
import io.cebes.http.server.HttpJsonProtocol._
import io.cebes.http.server.auth.AuthHandler
import io.cebes.http.server.routes.ApiErrorHandler
import io.cebes.http.server.routes.result.ResultHandler
import io.cebes.http.server.{HttpServer, VersionResponse}
import io.cebes.prop.{Prop, Property}
import io.cebes.server.routes.df.DataframeHandler
import io.cebes.server.routes.model.ModelHandler
import io.cebes.server.routes.pipeline.PipelineHandler
import io.cebes.server.routes.storage.StorageHandler
import io.cebes.server.routes.test.TestHandler

import scala.concurrent.ExecutionContextExecutor

class CebesHttpServer @Inject()(@Prop(Property.HTTP_INTERFACE) override val httpInterface: String,
                                @Prop(Property.HTTP_PORT) override val httpPort: Int,
                                @Prop(Property.HTTP_SERVER_SECRET) override val serverSecret: String,
                                override protected val refreshTokenStorage: CebesHttpRefreshTokenStorage,
                                override protected val authService: AuthService,
                                override protected val injector: Injector,
                                private val httpServerImplicits: HttpServerImplicits)
  extends HttpServer with AuthHandler with DataframeHandler with PipelineHandler with ModelHandler
    with StorageHandler with ResultHandler with TestHandler with ApiErrorHandler {

  protected implicit val actorSystem: ActorSystem = httpServerImplicits.actorSystem
  protected implicit val actorExecutor: ExecutionContextExecutor = actorSystem.dispatcher
  protected implicit val actorMaterializer: ActorMaterializer = httpServerImplicits.actorMaterializer

  override val routes: Route =
    handleExceptions(cebesDefaultExceptionHandler) {
      pathPrefix(CebesHttpServer.API_VERSION) {
        authApi ~
          requiredCebesSession { _ =>
            dataframeApi ~
              pipelineApi ~
              modelApi ~
              storageApi ~
              resultApi ~
              testApi
          }
      } ~ (path("") & get) {
        getFromResource("public/index.html")
      } ~ (path("version") & get) {
        complete(VersionResponse(CebesHttpServer.API_VERSION))
      }
    }
}

object CebesHttpServer {
  val API_VERSION = "v1"
}