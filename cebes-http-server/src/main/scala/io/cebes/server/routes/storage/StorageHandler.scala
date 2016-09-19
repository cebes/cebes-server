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
import akka.stream.Materializer
import io.cebes.server.http.SecuredSession
import io.cebes.server.models.CebesJsonProtocol._
import io.cebes.server.models.ReadRequest
import io.cebes.storage.StorageService

import scala.concurrent.ExecutionContext

trait StorageHandler extends SecuredSession {

  val storageService: StorageService

  implicit val actorSystem: ActorSystem
  implicit val actorExecutor: ExecutionContext
  implicit val actorMaterializer: Materializer

  val storageApi = pathPrefix("storage") {
    myRequiredSession { session =>
      (path("read") & post) {
        entity(as[ReadRequest]) { readRequest =>
          implicit ctx => ctx.complete(new Read(storageService).run(readRequest))
        }
      } ~ (path("upload") & put) {
        entity(as[Multipart.FormData]) { formData =>
          complete(new Upload().run(formData))
        }
      }
    }
  }
}
