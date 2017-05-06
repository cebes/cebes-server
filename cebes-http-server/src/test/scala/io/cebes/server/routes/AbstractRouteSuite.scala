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

import akka.actor.Scheduler
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.marshalling.ToEntityMarshaller
import akka.http.scaladsl.model.{StatusCodes, headers => akkaHeaders}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import io.cebes.pipeline.json.PipelineDef
import io.cebes.server.client.{RemoteDataframe, ServerException}
import io.cebes.server.helpers.{CebesHttpServerTestInjector, TestDataHelper}
import io.cebes.server.http.HttpServer
import io.cebes.server.routes.auth.HttpAuthJsonProtocol._
import io.cebes.server.routes.auth.LoginRequest
import io.cebes.server.routes.df.DataframeRequest
import io.cebes.server.util.Retries
import org.scalatest.FunSuite
import spray.json.JsonFormat

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Awaitable, Future}
import scala.util.{Failure, Success, Try}

/**
  * Mother of all Route test, with helpers for using akka test-kit,
  * logging in and storing cookies, etc...
  */
abstract class AbstractRouteSuite extends FunSuite with TestDataHelper with ScalatestRouteTest {

  //TODO: implement a better way to load the data (e.g. create a HTTP endpoint for testing purpose)

  protected val server: HttpServer = CebesHttpServerTestInjector.instance[HttpServer]
  private val authHeaders = login()

  implicit val scheduler: Scheduler = system.scheduler


  ////////////////////////////////////////////////////////////////////
  // implement traits
  ////////////////////////////////////////////////////////////////////

  override def sendSql(sqlText: String): RemoteDataframe = {
    requestDf[String]("df/sql", sqlText)
  }

  ////////////////////////////////////////////////////////////////////
  // REST APIs
  ////////////////////////////////////////////////////////////////////

  def post[E, T](url: String, entity: E)(op: => T)(implicit emE: ToEntityMarshaller[E]): T =
    Post(s"/${Routes.API_VERSION}/$url", entity).withHeaders(authHeaders: _*) ~> server.routes ~> check {
      op
    }

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
    Post(s"/${Routes.API_VERSION}/auth/login", LoginRequest("foo", "bar")) ~> server.routes ~> check {
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
  }

  private def wait[T](awaitable: Awaitable[T]): T = Await.result(awaitable, Duration.Inf)

  protected def request[E, R](url: String, entity: E)
                             (implicit emE: ToEntityMarshaller[E], jsR: JsonFormat[R]): R = {
    wait(postAsync[E, R](url, entity))
  }

  protected def requestDf[E](url: String, entity: E)(implicit emE: ToEntityMarshaller[E],
                                                     jsDfr: JsonFormat[DataframeResponse]): RemoteDataframe = {
    val df = wait(postAsync[E, DataframeResponse](url, entity))
    RemoteDataframe(df.id, df.schema)
  }

  /**
    * Counting number of rows in the given dataframe
    */
  protected def count(df: RemoteDataframe)(implicit emE: ToEntityMarshaller[DataframeRequest]): Long = {
    request[DataframeRequest, Long]("df/count", DataframeRequest(df.id))
  }

  protected def requestPipeline[E](url: String, entity: E)(implicit emE: ToEntityMarshaller[E],
                                                           jsDfr: JsonFormat[PipelineDef]): PipelineDef = {
    wait(postAsync[E, PipelineDef](url, entity))
  }

}
