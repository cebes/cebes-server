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
import io.cebes.pipeline.json.{InferenceRequest, InferenceResponse}
import io.cebes.serving.common.DefaultPipelineJsonProtocol._
import io.cebes.serving.inject.ServingTestInjector
import org.scalatest.FunSuite
import spray.json.DefaultJsonProtocol._
import spray.json._

class CebesServingServerSuite extends FunSuite with TestClient {

  protected val server: HttpServer = ServingTestInjector.injector.getInstance(classOf[CebesServingServer])

  override protected lazy val serverRoutes: Route = server.routes
  override protected lazy val apiVersion: String = ""

  test("failed request") {
    val requestEntity = InferenceRequest("nonExist", Map("a" -> JsNumber(100)), Array("s1", "s2"))

    val ex1 = intercept[ServerException] {
      request[InferenceRequest, InferenceResponse]("inference", requestEntity)
    }
    assert(ex1.message.endsWith("Serving name not found: nonExist"))
    assert(ex1.statusCode.contains(StatusCodes.BadRequest))

    val ex2 = intercept[ServerException] {
      request[InferenceRequest, InferenceResponse]("inferencesync", requestEntity)
    }
    assert(ex2.message.endsWith("Serving name not found: nonExist"))
    assert(ex2.statusCode.contains(StatusCodes.BadRequest))
  }

  test("linear regression async") {

    val requestEntity1 = InferenceRequest("testLinearRegression",
      Map("s1:inputDf" -> JsObject(Map("data" ->
        JsArray(Array(JsObject(Map("viscosity" -> 0.1.toJson, "proof_cut" -> 2.toJson))).toVector)))),
      Array("s2:outputDf", "s2:model")
    )
    val response1 = request[InferenceRequest, InferenceResponse]("inference", requestEntity1)
    assert(response1.outputs.size === 2)
    val jsDf = response1.outputs("s2:outputDf").asJsObject
    assert(jsDf.fields.contains("schema"))
    assert(jsDf.fields.contains("data"))
    assert(jsDf.fields("schema").asJsObject.fields("fields").asInstanceOf[JsArray].elements.length === 4)
    assert(jsDf.fields("data").asInstanceOf[JsArray].elements.length === 1)

    val ex1 = intercept[ServerException] {
      val requestEntity = InferenceRequest("testLinearRegression",
        Map("s1:inputDf" -> JsNumber(100)),
        Array("s2:outputDf", "s2:model"))
      request[InferenceRequest, InferenceResponse]("inference", requestEntity)
    }
    assert(ex1.getMessage.contains("Cannot deserialize value 100 into type interface io.cebes.df.Dataframe"))

  }
}
