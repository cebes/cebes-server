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
package io.cebes.http.helper


import java.util.concurrent.TimeUnit

import akka.actor.Scheduler
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.marshalling.ToEntityMarshaller
import akka.http.scaladsl.model.{HttpHeader, HttpMethod, HttpMethods, StatusCodes}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.http.scaladsl.unmarshalling.FromResponseUnmarshaller
import io.cebes.http.Retries
import io.cebes.http.client.ServerException
import io.cebes.http.server.HttpJsonProtocol._
import io.cebes.http.server.{FailResponse, FutureResult, RequestStatuses, SerializableResult}
import org.scalatest.FunSuiteLike
import spray.json.JsonFormat

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Awaitable, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}


/**
  * Simple test client without authentication (logins and stuff).
  * For secured test client, use [[SecuredTestClient]].
  */
trait TestClient extends FunSuiteLike with ScalatestRouteTest {

  protected val serverRoutes: Route
  protected val apiVersion: String

  protected lazy val authHeaders: Seq[HttpHeader] = Seq()

  implicit val scheduler: Scheduler = system.scheduler

  private val testDefaultTimeout = Duration(20, TimeUnit.SECONDS)

  ////////////////////////////////////////////////////////////////////
  // REST APIs
  ////////////////////////////////////////////////////////////////////

  def post[E, T](url: String, entity: E)(op: => T)(implicit emE: ToEntityMarshaller[E]): T =
    Post(getUrl(url), entity).withHeaders(authHeaders: _*) ~> serverRoutes ~> check {
      op
    }

  def get[T](url: String)(implicit emT: FromResponseUnmarshaller[T], tag: ClassTag[T]): T =
    Get(getUrl(url)).withHeaders(authHeaders: _*) ~> serverRoutes ~> check {
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
    query(url, entity, HttpMethods.PUT)

  def delete[E, T](url: String, entity: E)(implicit emE: ToEntityMarshaller[E],
                                           emT: FromResponseUnmarshaller[T], tag: ClassTag[T]): T =
    query(url, entity, HttpMethods.DELETE)

  /**
    * Send an asynchronous command and wait for the result, using exponential-backoff
    */
  def postAsync[E, R](url: String, entity: E)
                     (implicit emE: ToEntityMarshaller[E], jfR: JsonFormat[R]): Future[R] = {
    post(url, entity) {
      implicit val timeout: Duration = testDefaultTimeout

      response.status match {
        case StatusCodes.OK =>
          responseAs[FutureResult]
        case code =>
          Try(responseAs[FailResponse]) match {
            case Success(fr: FailResponse) =>
              throw ServerException(None, fr.message.getOrElse(""), fr.stackTrace, Some(code))
            case Failure(_) =>
              throw ServerException(None, s"Failed request: ${responseEntity.toString}", None, Some(code))
          }
      }
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

  private def query[E, T](url: String, entity: E, method: HttpMethod)
                         (implicit emE: ToEntityMarshaller[E], emT: FromResponseUnmarshaller[T], tag: ClassTag[T]): T =
    new RequestBuilder(method)(getUrl(url), entity).withHeaders(authHeaders: _*) ~> serverRoutes ~> check {
      implicit val timeout: Duration = testDefaultTimeout
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

  protected def getUrl(path: String): String = {
    apiVersion match {
      case "" => s"/$path"
      case s => s"/$s/$path"
    }
  }

  protected def wait[T](awaitable: Awaitable[T]): T = Await.result(awaitable, Duration.Inf)

  protected def request[E, R](url: String, entity: E)
                             (implicit emE: ToEntityMarshaller[E], jsR: JsonFormat[R]): R = {
    wait(postAsync[E, R](url, entity))
  }
}
