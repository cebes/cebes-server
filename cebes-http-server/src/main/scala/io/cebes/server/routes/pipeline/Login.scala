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

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.google.inject.Inject
import io.cebes.http.client.Client
import io.cebes.http.server.operations.AsyncSerializableOperation
import io.cebes.http.server.result.ResultStorage
import io.cebes.prop.{Prop, Property}
import io.cebes.repository.client.AuthTokenHelper
import io.cebes.server.http.HttpServerImplicits

import scala.concurrent.{ExecutionContext, Future}

class Login @Inject()(override protected val resultStorage: ResultStorage,
                      private val httpServerImplicits: HttpServerImplicits,
                      @Prop(Property.DEFAULT_REPOSITORY_HOST) private val systemDefaultRepoHost: String,
                      @Prop(Property.DEFAULT_REPOSITORY_PORT) private val systemDefaultRepoPort: Int)
  extends AsyncSerializableOperation[PipelineRepoLoginRequest, PipelineRepoLoginResponse]
    with AuthTokenHelper {

  /**
    * Implement this to do the real work
    */
  override protected def runImpl(requestEntity: PipelineRepoLoginRequest)
                                (implicit ec: ExecutionContext): Future[PipelineRepoLoginResponse] = {
    Future {
      val host = requestEntity.host.getOrElse(systemDefaultRepoHost)
      val port = requestEntity.port.getOrElse(systemDefaultRepoPort)

      implicit val actorSystem: ActorSystem = httpServerImplicits.actorSystem
      implicit val actorMaterializer: ActorMaterializer = httpServerImplicits.actorMaterializer

      new Client(host, port)
    }.flatMap { client =>
      client.login(requestEntity.userName, requestEntity.passwordHash).map { _ =>
        val token = encode(client.getRequestHeaders)
        PipelineRepoLoginResponse(client.host, client.port, token)
      }
    }
  }
}
