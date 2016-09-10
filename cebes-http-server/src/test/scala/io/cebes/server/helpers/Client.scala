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
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import akka.http.scaladsl.unmarshalling.{Unmarshal, _}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/**
  * Represent a HTTP connection to server (with security tokens and so on)
  */
class Client {

  val apiVersion = "v1"
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
    cebesRequest(RequestBuilding.Post(s"/$apiVersion/$uri", content)).flatMap { response =>
      response.status match {
        case StatusCodes.OK => Unmarshal(response.entity).to[ResponseType]
        case _ => Future.failed(new IOException(s"FAILED: ${response.status}"))
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
