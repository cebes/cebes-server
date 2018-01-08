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
import io.cebes.http.server.operations.AsyncSerializableOperation
import io.cebes.http.server.result.ResultStorage
import io.cebes.pipeline.PipelineService
import io.cebes.prop.{Prop, Property}
import io.cebes.repository.client.RepositoryClientFactory
import io.cebes.server.http.HttpServerImplicits
import io.cebes.spark.json.CebesSparkJsonProtocol.pipelineExportDefFormat

import scala.concurrent.{ExecutionContext, Future}

/**
  * Push a pipeline to a repository
  */
class Push @Inject()(override protected val resultStorage: ResultStorage,
                     private val pipelineService: PipelineService,
                     private val repoClientFactory: RepositoryClientFactory,
                     private val httpServerImplicits: HttpServerImplicits,
                     @Prop(Property.DEFAULT_REPOSITORY_HOST) private val systemDefaultRepoHost: String,
                     @Prop(Property.DEFAULT_REPOSITORY_PORT) private val systemDefaultRepoPort: Int)
  extends AsyncSerializableOperation[PipelinePushRequest, String] {

  /**
    * Implement this to do the real work
    */
  override protected def runImpl(requestEntity: PipelinePushRequest)
                                (implicit ec: ExecutionContext): Future[String] = {
    Future {
      pipelineService.get(requestEntity.tag.toString)
    }.flatMap { ppl =>
      val repoHost = requestEntity.host.getOrElse(systemDefaultRepoHost)
      val repoPort = requestEntity.port.getOrElse(systemDefaultRepoPort)
      val fullTag = requestEntity.tag.withDefaultServer(repoHost, repoPort)

      implicit val actorSystem: ActorSystem = httpServerImplicits.actorSystem
      implicit val actorMaterializer: ActorMaterializer = httpServerImplicits.actorMaterializer

      val client = repoClientFactory.get(None, None, requestEntity.token)
      val futureResult = client.createRepoIfNotExist(fullTag).flatMap { _ =>
        client.upload(ppl, fullTag)
      }
      futureResult.onFailure {
        case ex =>
          logger.error(s"Failed to push the pipeline to ${fullTag.toString}", ex)
      }
      futureResult
    }
  }
}
