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

import akka.actor.Scheduler
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.marshalling.ToEntityMarshaller
import akka.http.scaladsl.model.{HttpMethod, HttpMethods, StatusCodes, headers => akkaHeaders}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.http.scaladsl.unmarshalling.FromResponseUnmarshaller
import io.cebes.http.Retries
import io.cebes.http.client.ServerException
import io.cebes.http.server.HttpJsonProtocol._
import io.cebes.http.server.auth.HttpAuthJsonProtocol._
import io.cebes.http.server.auth.LoginRequest
import io.cebes.http.server.{FailResponse, FutureResult, RequestStatuses, SerializableResult}
import org.scalatest.FunSuiteLike
import spray.json.JsonFormat

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Awaitable, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

trait TestClient extends FunSuiteLike with ScalatestRouteTest {

  protected val serverRoutes: Route
  protected val apiVersion: String

  private lazy val authHeaders = login()

  implicit val scheduler: Scheduler = system.scheduler

  ////////////////////////////////////////////////////////////////////
  // REST APIs
  ////////////////////////////////////////////////////////////////////

  def post[E, T](url: String, entity: E)(op: => T)(implicit emE: ToEntityMarshaller[E]): T =
    Post(s"/$apiVersion/$url", entity).withHeaders(authHeaders: _*) ~> serverRoutes ~> check {
      op
    }

  def get[T](url: String)(implicit emT: FromResponseUnmarshaller[T], tag: ClassTag[T]): T =
    Get(s"/$apiVersion/$url").withHeaders(authHeaders: _*) ~> serverRoutes ~> check {
      status match {
        case StatusCodes.OK => responseAs[T]
        case StatusCodes.NotFound | StatusCodes.BadRequest | StatusCodes.InternalServerError =>
          // handled server exceptions
          val failResponse = responseAs[FailResponse]
          throw ServerException(None, failResponse.message.getOrElse(""), failResponse.stackTrace)
        case _ =>
          throw ServerException(None, response.toString(), None)
      }
    }

  def _query[E, T](url: String, entity: E, method: HttpMethod)(implicit emE: ToEntityMarshaller[E],
                                                               emT: FromResponseUnmarshaller[T], tag: ClassTag[T]): T =
    new RequestBuilder(method)(s"/$apiVersion/$url", entity).withHeaders(authHeaders: _*) ~> serverRoutes ~> check {
      status match {
        case StatusCodes.OK => responseAs[T]
        case StatusCodes.NotFound | StatusCodes.BadRequest | StatusCodes.InternalServerError =>
          // handled server exceptions
          val failResponse = responseAs[FailResponse]
          throw ServerException(None, failResponse.message.getOrElse(""), failResponse.stackTrace)
        case _ =>
          throw ServerException(None, response.toString(), None)
      }
    }

  def put[E, T](url: String, entity: E)(implicit emE: ToEntityMarshaller[E],
                                        emT: FromResponseUnmarshaller[T], tag: ClassTag[T]): T =
    _query(url, entity, HttpMethods.PUT)

  def delete[E, T](url: String, entity: E)(implicit emE: ToEntityMarshaller[E],
                                           emT: FromResponseUnmarshaller[T], tag: ClassTag[T]): T =
    _query(url, entity, HttpMethods.DELETE)

  /**
    * Send an asynchronous command and wait for the result, using exponential-backoff
    */
  def postAsync[E, R](url: String, entity: E)
                     (implicit emE: ToEntityMarshaller[E],
                      jfR: JsonFormat[R]): Future[R] = {
    post(url, entity) {
      responseAs[FutureResult]
    } ~> { futureResult =>

      Retries.retryUntil(Retries.expBackOff(100, max_count = 6))(
        post(s"request/${futureResult.requestId}", "")(responseAs[SerializableResult])
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
          case RequestStatuses.FINISHED =>
            assert(serializableResult.response.nonEmpty)
            serializableResult.response.get.convertTo[R]
          case s => throw new IllegalStateException(s"Invalid result status: $s")
        }
      }
    }
  }

  ////////////////////////////////////////////////////////////////////
  // Helpers
  ////////////////////////////////////////////////////////////////////

  private def login() = {
    Post(s"/$apiVersion/auth/login", LoginRequest("foo", "bar")) ~> serverRoutes ~> check {
      assert(status == StatusCodes.OK)
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
  }

  protected def wait[T](awaitable: Awaitable[T]): T = Await.result(awaitable, Duration.Inf)

  protected def request[E, R](url: String, entity: E)
                             (implicit emE: ToEntityMarshaller[E], jsR: JsonFormat[R]): R = {
    wait(postAsync[E, R](url, entity))
  }
}