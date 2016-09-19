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

package io.cebes.server.common

import java.io.{PrintWriter, StringWriter}

import akka.actor.ActorSystem
import akka.http.scaladsl.server.RequestContext
import io.cebes.server.models.{FailResponse, FutureResult, Request, Result}
import io.cebes.server.result.{ResultActorProducer, SerializableResult}
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
  * @tparam E Type of the request entity
  * @tparam T Type of the actual result
  * @tparam R Type of the result will be returned to client
  */
trait AsyncExecutor[E, T, R] {

  /**
    * Implement this to do the real work
    */
  def runImpl(requestEntity: E)(implicit ec: ExecutionContext): Future[T]

  /**
    * Transform the actual result (of type T)
    * into something that will be returned to the clients
    * Normally R should be Json-serializable.
    *
    * @param requestEntity The request entity
    * @param result        The actual result, returned by `runImpl`
    * @return a JSON-serializable object, to be returned to the clients
    */
  def transformResult(requestEntity: E, result: T): R

  def run(requestEntity: E)
         (implicit ec: ExecutionContext,
          ctx: RequestContext,
          actorSystem: ActorSystem,
          jfE: JsonFormat[E],
          jfR: JsonFormat[R],
          jfResult: JsonFormat[Result[E, R]],
          jfFr: JsonFormat[FailResponse],
          jfResultFail: JsonFormat[Result[E, FailResponse]]): FutureResult = {

    val requestObj = Request(requestEntity,
      ctx.request.uri.path.toString(), java.util.UUID.randomUUID())

    this.runImpl(requestEntity).onComplete {
      case Success(t) =>
        saveResult(requestObj, this.transformResult(requestEntity, t))

      case Failure(t) =>
        val sw = new StringWriter()
        val pw = new PrintWriter(sw)
        t.printStackTrace(pw)
        saveResult(requestObj, FailResponse(t.getMessage, sw.toString))
    }
    FutureResult(requestObj.requestId)
  }

  def saveResult[ResponseType](request: Request[E], response: ResponseType)
                              (implicit actorSystem: ActorSystem,
                               jfE: JsonFormat[E],
                               jfR: JsonFormat[ResponseType],
                               jfResult: JsonFormat[Result[E, ResponseType]]) = {
    val actor = actorSystem.actorOf(ResultActorProducer.props)
    actor ! SerializableResult(Result(request, response))
  }
}
