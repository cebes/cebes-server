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
 * Created by phvu on 10/09/16.
 */

package io.cebes.http.client

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding
import akka.http.scaladsl.client.RequestBuilding.RequestBuilder
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.marshalling.ToEntityMarshaller
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshal}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{ActorMaterializer, OverflowStrategy, QueueOfferResult}
import com.typesafe.scalalogging.LazyLogging
import io.cebes.http.server.HttpJsonProtocol._
import io.cebes.http.server.auth.HttpAuthJsonProtocol._
import io.cebes.http.server.auth.LoginRequest
import io.cebes.http.server.{FailResponse, FutureResult, RequestStatuses, SerializableResult}
import spray.json.JsonFormat

import scala.collection.immutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Random, Success, Try}

/**
  * Represent a HTTP connection to server (with security tokens and so on)
  */
class Client(host: String, port: Int)(implicit actorSystem: ActorSystem,
                                      actorMaterializer: ActorMaterializer) extends LazyLogging {

  // http://kazuhiro.github.io/scala/akka/akka-http/akka-streams/
  // 2016/01/31/connection-pooling-with-akka-http-and-source-queue.html
  private lazy val cebesPoolFlow = Http().cachedHostConnectionPool[Promise[HttpResponse]](host, port)

  private lazy val cebesQueue = Source.queue[(HttpRequest, Promise[HttpResponse])](10, OverflowStrategy.dropNew)
    .via(cebesPoolFlow).toMat(Sink.foreach({
    case ((Success(resp), p)) => p.success(resp)
    case ((Failure(e), p)) => p.failure(e)
  }))(Keep.left).run

  @volatile private var apiVersion: Option[String] = None

  private def cebesRequest(request: HttpRequest)(implicit ec: ExecutionContext): Future[HttpResponse] = {
    val promise = Promise[HttpResponse]
    cebesQueue.offer(request -> promise).flatMap {
      case QueueOfferResult.Enqueued => promise.future
      case _ => Future.failed(new RuntimeException("Failed to enqueue the request"))
    }
  }

  @volatile private var requestHeaders: immutable.Seq[HttpHeader] = immutable.Seq.empty[HttpHeader]


  /**
    * Send a POST request.
    * This is an alias of [[requestAndWait()]] with method = [[HttpMethods.POST]]
    */
  def postAndWait[RequestType, ResponseType](uri: String,
                                             content: RequestType)
                                            (implicit ma: ToEntityMarshaller[RequestType],
                                             jfResponse: JsonFormat[ResponseType],
                                             ec: ExecutionContext): Option[ResponseType] =
    requestAndWait(HttpMethods.POST, uri, content)(ma, jfResponse, ec)

  /**
    * Send an asynchronous request, and wait for a result using exponential-backoff.
    * This is mainly designed to get results of Cebes' asynchronous commands.
    *
    * @param method  HTTP method to use
    * @param uri     the URI of the Cebes server, without address and API version
    * @param content the message sent along this request
    * @tparam RequestType  type of the request message
    * @tparam ResponseType type of the expected response
    * @return
    */
  def requestAndWait[RequestType, ResponseType](method: HttpMethod, uri: String,
                                                content: RequestType)
                                               (implicit ma: ToEntityMarshaller[RequestType],
                                                jfResponse: JsonFormat[ResponseType],
                                                ec: ExecutionContext): Option[ResponseType] = {
    val futureResult = requestSync[RequestType, FutureResult](method, uri, content)

    val result = wait(futureResult)
    result.status match {
      case RequestStatuses.FINISHED =>
        result.response.map(_.convertTo[ResponseType])
      case s =>
        throw new RuntimeException(s"Request status: $s")
    }
  }

  /**
    * Implements Exponential backoff to wait for a [[FutureResult]]
    *
    * @param futureResult FutureResult object
    */
  def wait(futureResult: FutureResult)(implicit ma: ToEntityMarshaller[String],
                                       ec: ExecutionContext): SerializableResult = {
    var cnt = 0
    val MAX_COUNT = 4
    val DELTA = 1000 // in milliseconds

    while (cnt < 200) {
      val result = Try(requestSync[String, SerializableResult](
        HttpMethods.POST, s"request/${futureResult.requestId}", ""))
      result match {
        case Success(serializableResult) =>
          serializableResult.status match {
            case RequestStatuses.FAILED =>
              // TODO: decide on whether we should throw exception here
              // try to throw an exception if it is a FailResponse
              serializableResult.response match {
                case Some(responseEntity) =>
                  Try(responseEntity.convertTo[FailResponse]) match {
                    case Success(fr) =>
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
              return serializableResult
            case _ =>
              cnt += 1
              Thread.sleep(DELTA * ((1 << Random.nextInt(math.min(cnt, MAX_COUNT))) - 1))
          }
        case Failure(e) => throw e
      }
    }
    throw new IllegalArgumentException(s"Timed out after 100 trials " +
      s"getting result of request ${futureResult.requestId}")
  }

  /**
    * Post a message and block until the response is available
    * See the doc of [[Client.requestAsync]] for important notices regarding how to use this function.
    *
    * @param method  HTTP method to be used
    * @param uri     the URI of the Cebes server, without address and API version
    * @param content the message sent along this request
    * @tparam RequestType  type of the request message
    * @tparam ResponseType type of the expected response
    * @return the response
    */
  def requestSync[RequestType, ResponseType](method: HttpMethod, uri: String, content: RequestType,
                                             atMost: Duration = Duration(30, TimeUnit.SECONDS))
                                            (implicit ma: ToEntityMarshaller[RequestType],
                                             ua: FromEntityUnmarshaller[ResponseType],
                                             ec: ExecutionContext): ResponseType = {
    val futureResult = requestAsync(method, uri, content)(ma, ua, ec)
    Await.result(futureResult, atMost)
  }

  /**
    * Post a request to server.
    * Note that the types of the request and response messages are generic,
    * and we use implicits to make sure they can be marshaled/unmarshaled.
    *
    * What it means is that for the callers of this method, make sure you import those:
    *
    * import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
    * import io.cebes.http.server.models.HttpJsonProtocol._
    *
    * This is intended to be used for cebes' HTTP servers, since it constructs
    * the request object using `apiVersion` and any request headers received so far.
    *
    * @param method  the HTTP method to be used
    * @param uri     the URI of the Cebes server, without address and API version
    * @param content the message sent along this request
    * @tparam RequestType  type of the request message
    * @tparam ResponseType type of the expected response
    * @return a Future.
    */
  def requestAsync[RequestType, ResponseType](method: HttpMethod, uri: String, content: RequestType)
                                             (implicit ma: ToEntityMarshaller[RequestType],
                                              ua: FromEntityUnmarshaller[ResponseType],
                                              ec: ExecutionContext): Future[ResponseType] = {
    if (apiVersion.isEmpty) {
      apiVersion = Some(getServerVersion())
    }
    val request = new RequestBuilder(method).apply(
      s"/${apiVersion.getOrElse("")}/$uri", content).withHeaders(requestHeaders)
    sendRequest[RequestType, ResponseType](request)
  }

  /**
    * Most low-level function in this class, sending the already constructed `request` to the server
    * and return the unmarshaled response.
    */
  def sendRequest[RequestType, ResponseType](request: HttpRequest)
                                            (implicit ma: ToEntityMarshaller[RequestType],
                                             ua: FromEntityUnmarshaller[ResponseType],
                                             ec: ExecutionContext): Future[ResponseType] = {
    cebesRequest(request).flatMap { response =>
      response.status match {
        case StatusCodes.OK =>
          response.headers.filter(_.name().startsWith("Set-")) match {
            case x: Seq[HttpHeader] if x.nonEmpty =>
              this.requestHeaders = x.flatMap {
                case headers.`Set-Cookie`(c) => c.name.toUpperCase() match {
                  case "XSRF-TOKEN" =>
                    Seq(headers.RawHeader("X-XSRF-TOKEN", c.value), headers.Cookie(c.name, c.value))
                  case _ => Seq(headers.Cookie(c.name, c.value))
                }
                case h =>
                  Seq(headers.RawHeader(h.name().substring(4), h.value()))
              }
            case _ =>
          }
          Unmarshal(response.entity).to[ResponseType]
        case statusCode =>
          Unmarshal(response.entity).to[FailResponse].flatMap { failResponse =>
            logger.error(s"Failed result for request ${request.uri}")
            logger.error(s"Response: ${response.status} - ${failResponse.message}")
            Future.failed(ServerException(None, failResponse.message.getOrElse(""),
              failResponse.stackTrace, Some(statusCode)))
          }.recoverWith {
            case _ =>
              Unmarshal(response.entity).to[String].flatMap { msg =>
                logger.error(s"Failed result for request ${request.uri}")
                logger.error(s"Response: ${response.status} - $msg")
                Future.failed(ServerException(None, msg, None, Some(statusCode)))
              }
          }
      }
    }
  }

  /**
    * Login to the server with the given username and password.
    * Only applicable to Cebes HTTP servers.
    */
  def login(userName: String, passwordHash: String)
           (implicit ec: ExecutionContext): Future[String] = {
    val request = RequestBuilding.Post(s"/$apiVersion/auth/login", LoginRequest(userName, passwordHash))
    sendRequest[LoginRequest, String](request)
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // private helpers
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
    * Find out the server API version by querying it's `/version` endpoint.
    */
  private def getServerVersion()(implicit ec: ExecutionContext,
                                 uaFail: FromEntityUnmarshaller[FailResponse]): String = {
    val version = sendRequest[String, String](RequestBuilding.Get("/version", "")).recoverWith {
      case serverEx: ServerException if serverEx.statusCode.contains(StatusCodes.NotFound) => Future.successful("")
    }
    Await.result(version, Duration(20, TimeUnit.SECONDS))
  }
}
