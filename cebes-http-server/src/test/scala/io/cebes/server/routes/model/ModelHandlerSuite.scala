/* Copyright 2017 The Cebes Authors. All Rights Reserved.
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
package io.cebes.server.routes.model

import java.util.UUID

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import io.cebes.pipeline.json._
import io.cebes.server.client.ServerException
import io.cebes.server.routes.AbstractRouteSuite
import io.cebes.server.routes.HttpJsonProtocol._
import io.cebes.server.routes.common.HttpTagJsonProtocol._
import io.cebes.server.routes.common._
import io.cebes.server.routes.df.HttpDfJsonProtocol._
import io.cebes.server.routes.df.{DropNARequest, LimitRequest, SampleRequest}
import io.cebes.spark.json.CebesSparkJsonProtocol._
import io.cebes.tag.Tag
import spray.json.DefaultJsonProtocol._


/**
  * Test suite for [[ModelHandler]]
  */
class ModelHandlerSuite extends AbstractRouteSuite {

  private lazy val pipelineId = {
    val pplDef = PipelineDef(None, Array(
      StageDef("s1", "VectorAssembler", Map(
        "inputCols" -> ValueDef(Array("viscosity", "proof_cut")),
        "outputCol" -> ValueDef("features"))),
      StageDef("s2", "LinearRegression", Map(
        "inputDf" -> StageOutputDef("s1", "outputDf"),
        "featuresCol" -> ValueDef("features"),
        "labelCol" -> ValueDef("caliper"),
        "predictionCol" -> ValueDef("caliper_predict"),
        "elasticNetParam" -> ValueDef(0.01)))
    ))
    val pplResult = requestPipeline("pipeline/create", pplDef)
    assert(pplResult.id.nonEmpty)
    assert(pplResult.stages.length === 2)
    pplResult.id.get
  }

  private lazy val modelId = {
    val dfIn = requestDf("df/dropna", DropNARequest(
      requestDf("df/limit", LimitRequest(getCylinderBands.id, 200)).id,
      3, Array("viscosity", "proof_cut", "caliper")))

    val runDefMultOutputs = PipelineRunDef(PipelineDef(Some(pipelineId), Array()),
      Map("s1:inputDf" -> DataframeMessageDef(dfIn.id)),
      Array(StageOutputDef("s2", "model")))

    val runResult = request[PipelineRunDef, Array[(StageOutputDef, PipelineMessageDef)]]("pipeline/run",
      runDefMultOutputs)
    assert(runResult.length === 1)
    assert(runResult(0)._2.isInstanceOf[ModelMessageDef])
    runResult(0)._2.asInstanceOf[ModelMessageDef].modelId
  }

  test("create and get model") {
    val modelDef = requestModel("model/get", modelId.toString)
    assert(modelDef.id === modelId)
    assert(modelDef.inputs("elasticNetParam").isInstanceOf[ValueDef])
    assert(modelDef.inputs("elasticNetParam").asInstanceOf[ValueDef].value.isInstanceOf[Double])
    assert(modelDef.inputs("elasticNetParam").asInstanceOf[ValueDef].value.asInstanceOf[Double] === 0.01)

    // cannot get non-exist model
    val ex0 = intercept[ServerException] {
      requestModel("model/get", UUID.randomUUID().toString)
    }
    assert(ex0.message.startsWith("ID not found:"))
  }

  test("create, tag and untag") {

    try {
      requestModel("model/tagdelete", TagDeleteRequest(Tag.fromString("tag1:latest")))
    } catch {
      case _: ServerException =>
    }

    // random UUID
    val ex0 = intercept[ServerException] {
      requestModel("model/tagadd", TagAddRequest(Tag.fromString("tag1"), UUID.randomUUID()))
    }
    assert(ex0.getMessage.startsWith("Object ID not found:"))

    // valid request
    requestModel("model/tagadd", TagAddRequest(Tag.fromString("tag1"), modelId))

    // duplicated tag
    val ex1 = intercept[ServerException] {
      requestModel("model/tagadd", TagAddRequest(Tag.fromString("tag1"), modelId))
    }
    assert(ex1.getMessage.startsWith("Tag tag1:latest already exists"))

    // get model by tag
    val model = requestModel("model/get", "tag1:latest")
    assert(model.id === modelId)

    // get all the tags
    val tags = request[TagsGetRequest, Array[TaggedModelResponse]]("model/tags", TagsGetRequest(None, 10))
    assert(tags.length === 1)
    assert(tags(0).tag.toString === "tag1:latest")

    val tags1 = request[TagsGetRequest, Array[TaggedModelResponse]]("model/tags",
      TagsGetRequest(Some("randomstuff???"), 10))
    assert(tags1.length === 0)

    // delete tag
    requestModel("model/tagdelete", TagDeleteRequest(Tag.fromString("tag1:latest")))

    val tags2 = request[TagsGetRequest, Array[TaggedModelResponse]]("model/tags", TagsGetRequest(None, 10))
    assert(tags2.length === 0)

    // cannot get the tag again
    val ex3 = intercept[ServerException](requestModel("model/get", "tag1:latest"))
    assert(ex3.getMessage.startsWith("Tag not found: tag1:latest"))

    // cannot delete non-existed tag
    val ex4 = intercept[ServerException](requestModel("model/tagdelete",
      TagDeleteRequest(Tag.fromString("tag1:latest"))))
    assert(ex4.getMessage.startsWith("Tag not found: tag1:latest"))

    // but can get the Dataframe using its ID
    val model2 = requestModel("model/get", modelId.toString)
    assert(model2 ne model)
    assert(model2.id === modelId)
  }

  test("run a model - successful cases") {

    // prepare the dataframe that can be fed into the model
    val dfIn = requestDf("df/dropna", DropNARequest(
      requestDf("df/sample", SampleRequest(getCylinderBands.id, withReplacement = true, 0.5, 42)).id,
      3, Array("viscosity", "proof_cut", "caliper")))

    val pplRunDef = PipelineRunDef(PipelineDef(Some(pipelineId), Array()),
      Map("s1:inputDf" -> DataframeMessageDef(dfIn.id)),
      Array(StageOutputDef("s1", "outputDf")))
    val preprocessedDfResult = request[PipelineRunDef, Array[(StageOutputDef, PipelineMessageDef)]](
      "pipeline/run", pplRunDef)
    assert(preprocessedDfResult.length === 1)
    val preprocessedDfId = preprocessedDfResult(0)._2.asInstanceOf[DataframeMessageDef].dfId

    // feed the preprocessed Dataframe into the model
    val runDef = ModelRunDef(ModelMessageDef(modelId), DataframeMessageDef(preprocessedDfId))

    val runResult = requestDf("model/run", runDef)
    assert(runResult.schema.contains("caliper_predict"))
    assert(runResult.schema.contains("features"))

    // make sure the result is cached
    val dfResult2 = requestDf("df/get", runResult.id.toString)
    assert(dfResult2 ne runResult)
    assert(dfResult2.id === runResult.id)
    assert(dfResult2.schema === runResult.schema)

    val runDefFail = ModelRunDef(ModelMessageDef(modelId), DataframeMessageDef(UUID.randomUUID()))
    val ex = intercept[ServerException](requestDf("model/run", runDefFail))
    assert(ex.message.startsWith("ID not found"))
  }
}
