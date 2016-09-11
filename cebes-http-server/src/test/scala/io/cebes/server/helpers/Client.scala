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
import akka.http.scaladsl.marshalling._
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.{Unmarshal, _}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}

import scala.collection.immutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/**
  * Represent a HTTP connection to server (with security tokens and so on)
  */
class Client {

  val apiVersion = "v1"
  var requestHeaders: immutable.Seq[HttpHeader] = immutable.Seq.empty[HttpHeader]
  //var requestCookies: immutable.Seq[HttpHeader]

  implicit val system = ActorSystem("CebesClientApp")
  implicit val materializer = ActorMaterializer()

  lazy val cebesConnectionFlow: Flow[HttpRequest, HttpResponse, Any] =
    Http().outgoingConnection(HttpServerTest.httpInterface, HttpServerTest.httpPort)

  def cebesRequest(request: HttpRequest): Future[HttpResponse] =
    Source.single(request).via(cebesConnectionFlow).runWith(Sink.head)

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
  def postAsync[RequestType, ResponseType]
  (uri: String, content: RequestType)(implicit ma: ToEntityMarshaller[RequestType],
                                      ua: FromEntityUnmarshaller[ResponseType],
                                      ec: ExecutionContext): Future[ResponseType] = {
    val request = RequestBuilding.Post(s"/$apiVersion/$uri", content).withHeaders(requestHeaders)
      .withHeaders(headers.RawHeader("sdasd", "asdasd"))
    cebesRequest(request).flatMap { response =>
      response.status match {
        case StatusCodes.OK =>
          requestHeaders = response.headers.filter(_.name().startsWith("Set-")).map {
            case headers.`Set-Cookie`(c) =>
              println(s"============= Cookie ${c.name}: ${c.value}")
              headers.Cookie(c.name, c.value)
            case h if h.name().startsWith("Set-") =>
              headers.Cookie(h.name().substring(4), h.value())
          }
          Unmarshal(response.entity).to[ResponseType]
        case _ =>
          val msg = Await.result(Unmarshal(response.entity).to[String], Duration(10, TimeUnit.SECONDS))
          Future.failed(new IOException(s"FAILED: ${response.status}: $msg"))
      }
    }
  }

  def post[RequestType, ResponseType]
  (uri: String, content: RequestType)(implicit ma: ToEntityMarshaller[RequestType],
                                      ua: FromEntityUnmarshaller[ResponseType],
                                      ec: ExecutionContext): ResponseType = {
    Await.result(postAsync(uri, content), Duration(1, TimeUnit.MINUTES))
  }
}
