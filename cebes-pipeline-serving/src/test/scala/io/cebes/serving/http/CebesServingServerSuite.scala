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
package io.cebes.serving.http

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import io.cebes.http.client.ServerException
import io.cebes.http.helper.TestClient
import io.cebes.http.server.HttpServer
import io.cebes.pipeline.json.{InferenceRequest, InferenceResponse, ValueDef}
import io.cebes.serving.DefaultPipelineJsonProtocol._
import io.cebes.serving.inject.CebesServingInjector
import org.scalatest.FunSuite

class CebesServingServerSuite extends FunSuite with TestClient {

  protected val server: HttpServer = CebesServingInjector.instance[CebesServingServer]

  override protected lazy val serverRoutes: Route = server.routes
  override protected lazy val apiVersion: String = ""

  test("failed request") {
    val requestEntity = InferenceRequest("nonExist", Map("a" -> ValueDef(100)), Array("s1", "s2"))

    val ex1 = intercept[ServerException] {
      request[InferenceRequest, InferenceResponse]("inferenceSync",requestEntity )
    }
    assert(ex1.message.endsWith("Serving name not found: nonExist"))
    assert(ex1.statusCode.contains(StatusCodes.BadRequest))

    val ex2 = intercept[ServerException] {
      request[InferenceRequest, InferenceResponse]("inference",requestEntity )
    }
    assert(ex2.message.endsWith("Serving name not found: nonExist"))
    assert(ex2.statusCode.contains(StatusCodes.BadRequest))

  }
}
