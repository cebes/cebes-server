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
 *
 * Created by phvu on 05/09/16.
 */

package io.cebes.server.routes.storage

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.Multipart
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import io.cebes.server.http.SecuredSession
import io.cebes.server.inject.CebesHttpServerInjector
import io.cebes.server.models.CebesJsonProtocol._
import io.cebes.server.models.ReadRequest

import scala.concurrent.ExecutionContext

trait StorageHandler extends SecuredSession {

  implicit def actorSystem: ActorSystem

  implicit def actorExecutor: ExecutionContext

  implicit def actorMaterializer: Materializer

  val storageApi: Route = pathPrefix("storage") {
    myRequiredSession { _ =>
      (path("read") & post) {
        entity(as[ReadRequest]) { readRequest =>
          implicit ctx =>
            CebesHttpServerInjector.instance[Read].run(readRequest).flatMap(ctx.complete(_))
        }
      } ~ (path("upload") & put) {
        entity(as[Multipart.FormData]) { formData =>
          implicit ctx =>
            CebesHttpServerInjector.instance[Upload].run(formData).flatMap(ctx.complete(_))
        }
      }
    }
  }
}
