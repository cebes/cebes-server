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
package io.cebes.repository.http

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import com.google.inject.Inject
import io.cebes.http.server.HttpServer
import io.cebes.http.server.routes.ApiErrorHandler
import io.cebes.prop.{Prop, Property}
import io.cebes.repository.http.CebesRepositoryJsonProtocol._

import scala.concurrent.ExecutionContextExecutor

class CebesRepositoryServer @Inject()(@Prop(Property.REPOSITORY_INTERFACE) override val httpInterface: String,
                                      @Prop(Property.REPOSITORY_PORT) override val httpPort: Int,
                                      override protected val refreshTokenStorage: CebesRepositoryRefreshTokenStorage)
  extends HttpServer with ApiErrorHandler {

  override protected val serverSecret: String =
    "v8Km83QULVYHVgx0GxJKkZ7v3uhtA3wVY3maYArW5fI1WFTpUwyXQQLwGjVfirAA5OuIVv"

  protected implicit val actorSystem: ActorSystem = ActorSystem("CebesServerApp")
  protected implicit val actorExecutor: ExecutionContextExecutor = actorSystem.dispatcher
  protected implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()

  override val routes: Route =
    pathPrefix(CebesRepositoryServer.API_VERSION) {
      (path("catalog") & get) {
        complete("ok")
      }
    } ~
      (path("version") & get) {
        complete(VersionResponse(CebesRepositoryServer.API_VERSION))
      }
}

object CebesRepositoryServer {
  val API_VERSION = "v1"
}