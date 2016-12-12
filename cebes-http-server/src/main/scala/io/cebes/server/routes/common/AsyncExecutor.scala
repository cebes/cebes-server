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
 * Created by phvu on 17/09/16.
 */

package io.cebes.server.routes.common

import java.io.{PrintWriter, StringWriter}

import akka.actor.ActorSystem
import akka.http.scaladsl.server.RequestContext
import com.typesafe.scalalogging.LazyLogging
import io.cebes.server.models._
import io.cebes.server.result.ResultStorage
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  * The main workforce for long-running executors. This does the following:
  *
  * - Start the real work in a Future block
  * - Hook onComplete to the Future, and store the results
  * - Returns a FutureResult, basically contains the request ID
  *
  * Classes that extend this trait need to implement at least
  * runImp() and transformResult()
  *
  * @tparam E Type of the request entity
  * @tparam T Type of the actual result
  * @tparam R Type of the result will be returned to client
  */
trait AsyncExecutor[E, T, R] extends LazyLogging {

  /**
    * To be injected by the DI framework
    */
  val resultStorage: ResultStorage

  /**
    * Implement this to do the real work
    */
  protected def runImpl(requestEntity: E)(implicit ec: ExecutionContext): Future[T]

  /**
    * Transform the actual result (of type T)
    * into something that will be returned to the clients
    * Normally R should be Json-serializable.
    *
    * @param requestEntity The request entity
    * @param result        The actual result, returned by `runImpl`
    * @return a JSON-serializable object, to be returned to the clients
    */
  protected def transformResult(requestEntity: E, result: T): Option[R]

  def run(requestEntity: E)
         (implicit ec: ExecutionContext,
          ctx: RequestContext,
          actorSystem: ActorSystem,
          jfE: JsonFormat[E],
          jfR: JsonFormat[R],
          jfFr: JsonFormat[FailResponse]): Future[FutureResult] = {

    implicit val scheduler = actorSystem.scheduler
    val requestJson = Some(requestEntity.toJson)
    val requestId = java.util.UUID.randomUUID()

    resultStorage.saveWithRetry {
      SerializableResult(requestId, RequestStatuses.SCHEDULED, None, requestJson)
    }.map { _ =>
      runImpl(requestEntity).onComplete {
        case Success(t) =>
          resultStorage.saveWithRetry(SerializableResult(requestId, RequestStatuses.FINISHED,
            this.transformResult(requestEntity, t).map(_.toJson), requestJson)).onFailure {
            case f => logger.error(s"Failed to save FINISHED result for request $requestId", f)
          }

        case Failure(t) =>
          val sw = new StringWriter()
          val pw = new PrintWriter(sw)
          t.printStackTrace(pw)

          resultStorage.saveWithRetry(SerializableResult(requestId, RequestStatuses.FAILED,
            Some(FailResponse(Option(t.getMessage), Option(sw.toString)).toJson), requestJson)).onFailure {
            case f => logger.error(s"Failed to save FAILED result for request $requestId", f)
          }
      }
      FutureResult(requestId)
    }
  }
}
