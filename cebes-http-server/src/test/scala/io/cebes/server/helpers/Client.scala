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

package io.cebes.server.helpers

import java.io.IOException
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding
import akka.http.scaladsl.marshalling.ToEntityMarshaller
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshal}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{ActorMaterializer, OverflowStrategy, QueueOfferResult}

import scala.collection.immutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/**
  * Represent a HTTP connection to server (with security tokens and so on)
  */
class Client {

  implicit val actorSystem = Client.system
  implicit val actorMaterializer = Client.materializer

  // http://kazuhiro.github.io/scala/akka/akka-http/akka-streams/
  // 2016/01/31/connection-pooling-with-akka-http-and-source-queue.html
  lazy val cebesPoolFlow = Http().cachedHostConnectionPool[Promise[HttpResponse]](
    HttpServerTest.httpInterface, HttpServerTest.httpPort)

  lazy val cebesQueue = Source.queue[(HttpRequest, Promise[HttpResponse])](10, OverflowStrategy.dropNew)
    .via(cebesPoolFlow).toMat(Sink.foreach({
    case ((Success(resp), p)) => p.success(resp)
    case ((Failure(e), p)) => p.failure(e)
  }))(Keep.left).run

  def cebesRequest(request: HttpRequest)(implicit ec: ExecutionContext): Future[HttpResponse] = {
    val promise = Promise[HttpResponse]
    cebesQueue.offer(request -> promise).flatMap {
      case QueueOfferResult.Enqueued => promise.future
      case _ => Future.failed(new RuntimeException())
    }
  }

  @volatile var requestHeaders: immutable.Seq[HttpHeader] = immutable.Seq.empty[HttpHeader]


  /**
    * Post a message and block until the response is available
    * See the doc of [[Client.postAsync()]] for important notices regarding how to use this function.
    *
    * @param uri     the URI of the Cebes server, without address and API version
    * @param content the message sent along this request
    * @tparam RequestType  type of the request message
    * @tparam ResponseType type of the expected response
    * @return the response
    */
  def post[RequestType, ResponseType](uri: String, content: RequestType)
                                     (implicit ma: ToEntityMarshaller[RequestType],
                                      ua: FromEntityUnmarshaller[ResponseType],
                                      ec: ExecutionContext): ResponseType = {
    val futureResult = postAsync(uri, content)(ma, ua, ec)
    Await.result(futureResult, Duration(10, TimeUnit.SECONDS))
  }

  /**
    * Post a request to server.
    * Note that the types of the request and response messages are generic,
    * and we use implicits to make sure they can be marshaled/unmarshaled.
    *
    * What it means is that for the callers of this method, make sure you import those:
    *
    * import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
    * import io.cebes.server.models.CebesJsonProtocol._
    *
    * @param uri     the URI of the Cebes server, without address and API version
    * @param content the message sent along this request
    * @tparam RequestType  type of the request message
    * @tparam ResponseType type of the expected response
    * @return a Future.
    */
  def postAsync[RequestType, ResponseType](uri: String, content: RequestType)
                                          (implicit ma: ToEntityMarshaller[RequestType],
                                           ua: FromEntityUnmarshaller[ResponseType],
                                           ec: ExecutionContext): Future[ResponseType] = {

    val request = RequestBuilding.Post(s"/${Client.apiVersion}/$uri", content).withHeaders(requestHeaders)

    cebesRequest(request).flatMap { response =>
      response.status match {
        case StatusCodes.OK =>
          // always update request headers
          this.requestHeaders = response.headers.filter(_.name().startsWith("Set-")).flatMap {
            case headers.`Set-Cookie`(c) => c.name.toUpperCase() match {
              case "XSRF-TOKEN" =>
                Seq(headers.RawHeader("X-XSRF-TOKEN", c.value), headers.Cookie(c.name, c.value))
              case _ => Seq(headers.Cookie(c.name, c.value))
            }
            case h =>
              Seq(headers.RawHeader(h.name().substring(4), h.value()))
          }
          Unmarshal(response.entity).to[ResponseType]
        case _ =>
          val msg = Await.result(Unmarshal(response.entity).to[String], Duration(10, TimeUnit.SECONDS))
          Future.failed(new IOException(s"FAILED: ${response.status}: $msg"))
      }
    }
  }
}

object Client {

  val apiVersion = "v1"

  implicit val system = ActorSystem("CebesClientApp")
  implicit val materializer = ActorMaterializer()
}
