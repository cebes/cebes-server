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
 * Created by phvu on 14/12/2016.
 */

package io.cebes.server.routes

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{StatusCodes, headers => akkaHeaders}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import io.cebes.server.helpers.{ServerException, TestPropertyHelper}
import io.cebes.server.models._
import io.cebes.server.routes.df.CebesDfProtocol._
import io.cebes.server.util.Retries
import org.scalatest.FunSuite

import scala.util.{Failure, Success, Try}

abstract class AbstractRouteSuite extends FunSuite with TestPropertyHelper
  with ScalatestRouteTest with Routes {

  implicit val actorSystem = system
  implicit val actorMaterializer = materializer
  implicit val actorExecutor = executor
  implicit val scheduler = actorSystem.scheduler

  val authHeaders = Post("/v1/auth/login", UserLogin("foo", "bar")) ~> routes ~> check {
    assert(status === StatusCodes.OK)
    val responseCookies = headers.filter(_.name().startsWith("Set-"))
    assert(responseCookies.nonEmpty)

    responseCookies.flatMap {
      case akkaHeaders.`Set-Cookie`(c) => c.name.toUpperCase() match {
        case "XSRF-TOKEN" =>
          Seq(akkaHeaders.RawHeader("X-XSRF-TOKEN", c.value), akkaHeaders.Cookie(c.name, c.value))
        case _ => Seq(akkaHeaders.Cookie(c.name, c.value))
      }
      case h =>
        Seq(akkaHeaders.RawHeader(h.name().substring(4), h.value()))
    }
  }

  def post[E, T](url: String, entity: E)(op: => T) =
    Post(s"/v1/$url", entity).withHeaders(authHeaders) ~> routes ~> check {
      op
    }

  def requestAndWait[E](url: String, entity: E) = {
    post(url, entity) {
      val futureResult = responseAs[FutureResult]

      Retries.retryUntil(Retries.expBackOff(100, max_count = 6))(
        post(s"request/${futureResult.requestId}", "") {
          responseAs[SerializableResult]
        }
      )(_.status != RequestStatuses.SCHEDULED).map { serializableResult =>
        serializableResult.status match {
          case RequestStatuses.FAILED =>
            serializableResult.response match {
              case Some(responseEntity) =>
                Try(responseEntity.convertTo[FailResponse]) match {
                  case Success(fr: FailResponse) =>
                    throw ServerException(Some(serializableResult.requestId),
                      fr.message.getOrElse(""), fr.stackTrace)
                  case Failure(_) =>
                    // throw it as it is
                    throw ServerException(Some(serializableResult.requestId),
                      responseEntity.toString(), None)
                }
              case None =>
                throw ServerException(Some(serializableResult.requestId),
                  "Unknown server error", None)
            }
          case RequestStatuses.FINISHED => serializableResult
        }
      }
    }
  }
}
