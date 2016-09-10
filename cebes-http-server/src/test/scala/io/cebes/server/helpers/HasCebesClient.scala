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
 * Created by phvu on 07/09/16.
 */

package io.cebes.server.helpers

import java.io.IOException
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{RequestEntity, _}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import spray.json.{RootJsonReader, RootJsonWriter}

import scala.concurrent.{Await, Future}
import spray.json._
import DefaultJsonProtocol._
import io.cebes.server.models.CebesJsonProtocol._
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.server.ContentNegotiator.Alternative.ContentType
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

trait HasCebesClient extends FunSuite with BeforeAndAfterAll {

  val apiVersion = "v1"
  implicit val system = ActorSystem("CebesClientApp")
  implicit val materializer = ActorMaterializer()

  lazy val cebesConnectionFlow: Flow[HttpRequest, HttpResponse, Any] =
    Http().outgoingConnection(CebesHttpServerTest.httpInterface, CebesHttpServerTest.httpPort)

  def cebesRequest(request: HttpRequest): Future[HttpResponse] =
    Source.single(request).via(cebesConnectionFlow).runWith(Sink.head)

  def post[RequestType <: Object: JsonWriter, ResponseType <: Object: JsonReader]
  (uri: String, content: RequestType, jsonReader: RootJsonReader[ResponseType]): Future[ResponseType] = {
    val entity = HttpEntity(ContentTypes.`application/json`, content.toJson.compactPrint) //.asInstanceOf[RequestEntity]
    //val entity = Await.result(Marshal(content).to[RequestEntity], Duration(1, TimeUnit.MINUTES))
    cebesRequest(RequestBuilding.Post(s"/$apiVersion/$uri", entity)).flatMap { response =>
      response.status match {
        case StatusCodes.OK =>
          Future(sprayJsonUnmarshaller(jsonReader)().toJson.convertTo[ResponseType])
        case _ => Future.failed(new IOException(s"FAILED: ${response.status}"))
      }
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    CebesHttpServerTest.register()
  }

  override def afterAll(): Unit = {
    CebesHttpServerTest.unregister()
    super.afterAll()
  }
}
