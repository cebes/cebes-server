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
package io.cebes.server.routes.pipeline

import java.util.UUID

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import io.cebes.df.functions
import io.cebes.pipeline.json._
import io.cebes.server.client.ServerException
import io.cebes.server.routes.AbstractRouteSuite
import io.cebes.server.routes.HttpJsonProtocol._
import io.cebes.server.routes.common.HttpTagJsonProtocol._
import io.cebes.server.routes.common.{TagAddRequest, TagDeleteRequest, TaggedPipelineResponse, TagsGetRequest}
import io.cebes.spark.json.CebesSparkJsonProtocol._
import io.cebes.tag.Tag
import spray.json.DefaultJsonProtocol._

/**
  * Test suite for [[PipelineHandler]]
  */
class PipelineHandlerSuite extends AbstractRouteSuite {

  test("create and get pipeline") {
    val pplDef = PipelineDef(None, Array(StageDef("s1", "Alias", Map("alias" -> ValueDef("new_name")))))
    val pplResult = requestPipeline("pipeline/create", pplDef)
    assert(pplResult.id.nonEmpty)
    assert(pplResult.stages.length === 1)
    assert(pplResult.stages(0) === pplDef.stages(0))

    // get the created pipeline
    val pplDefb = requestPipeline("pipeline/get", pplResult.id.get.toString)
    assert(pplDefb.id.get === pplResult.id.get)
    assert(pplDefb.stages.length === 1)
    assert(pplDefb.stages(0) === pplResult.stages(0))

    // cannot get non-exist pipeline
    val ex0 = intercept[ServerException] {
      requestPipeline("pipeline/get", UUID.randomUUID().toString)
    }
    assert(ex0.message.startsWith("ID not found:"))

    val pplDef2 = PipelineDef(None, Array(StageDef("s1", "Alias", Map("alias_nonexists" -> ValueDef("new_name")))))
    val ex1 = intercept[ServerException] {
      requestPipeline("pipeline/create", pplDef2)
    }
    assert(ex1.message.contains("Input name alias_nonexists not found in stage Alias(name=s1)"))

    val pplDef3 = PipelineDef(None,
      Array(StageDef("s1", "Alias_nonexist", Map("alias_nonexists" -> ValueDef("new_name")))))
    val ex2 = intercept[ServerException] {
      requestPipeline("pipeline/create", pplDef3)
    }
    assert(ex2.message.contains("Stage class not found: Alias_nonexist"))
  }

  test("create, tag and untag") {

    try {
      requestPipeline("pipeline/tagdelete", TagDeleteRequest(Tag.fromString("tag1:latest")))
    } catch {
      case _: ServerException =>
    }

    // random UUID
    val ex0 = intercept[ServerException] {
      requestPipeline("pipeline/tagadd", TagAddRequest(Tag.fromString("tag1"), UUID.randomUUID()))
    }
    assert(ex0.getMessage.startsWith("Object ID not found:"))

    // create a pipeline
    val pplDef = PipelineDef(None, Array(StageDef("s1", "Alias", Map("alias" -> ValueDef("new_name")))))
    val pplResult = requestPipeline("pipeline/create", pplDef)
    assert(pplResult.id.nonEmpty)
    assert(pplResult.stages.length === 1)
    assert(pplResult.stages(0) === pplDef.stages(0))
    val pipelineId = pplResult.id.get

    // valid request
    requestPipeline("pipeline/tagadd", TagAddRequest(Tag.fromString("tag1"), pipelineId))

    // another pipeline
    val pplResult2 = requestPipeline("pipeline/create", pplDef)
    assert(pplResult2.id.get !== pplResult.id.get)

    // duplicated tag
    val ex1 = intercept[ServerException] {
      requestPipeline("pipeline/tagadd", TagAddRequest(Tag.fromString("tag1"), pplResult2.id.get))
    }
    assert(ex1.getMessage.startsWith("Tag tag1:latest already exists"))

    val ex2 = intercept[ServerException] {
      requestPipeline("pipeline/tagadd", TagAddRequest(Tag.fromString("tag1:latest"), pplResult2.id.get))
    }
    assert(ex2.getMessage.startsWith("Tag tag1:latest already exists"))

    // get pipeline by tag
    val ppl2 = requestPipeline("pipeline/get", "tag1:latest")
    assert(ppl2.id.get === pplResult.id.get)
    assert(ppl2.stages(0) === pplResult.stages(0))

    // get all the tags
    val tags = request[TagsGetRequest, Array[TaggedPipelineResponse]]("pipeline/tags", TagsGetRequest(None, 10))
    assert(tags.length === 1)
    assert(tags(0).tag.toString === "tag1:latest")

    val tags1 = request[TagsGetRequest, Array[TaggedPipelineResponse]]("pipeline/tags",
      TagsGetRequest(Some("randomstuff???"), 10))
    assert(tags1.length === 0)

    // delete tag
    requestPipeline("pipeline/tagdelete", TagDeleteRequest(Tag.fromString("tag1:latest")))

    val tags2 = request[TagsGetRequest, Array[TaggedPipelineResponse]]("pipeline/tags", TagsGetRequest(None, 10))
    assert(tags2.length === 0)

    // cannot get the tag again
    val ex3 = intercept[ServerException](requestPipeline("pipeline/get", "tag1:latest"))
    assert(ex3.getMessage.startsWith("Tag not found: tag1:latest"))

    // cannot delete non-existed tag
    val ex4 = intercept[ServerException](requestPipeline("pipeline/tagdelete",
      TagDeleteRequest(Tag.fromString("tag1:latest"))))
    assert(ex4.getMessage.startsWith("Tag not found: tag1:latest"))

    // but can get the Dataframe using its ID
    val ppl3 = requestPipeline("pipeline/get", pipelineId.toString)
    assert(ppl3 ne ppl2)
    assert(ppl3.id.get === pipelineId)
  }

  def simplePipeline: PipelineDef = {
    PipelineDef(None,
      Array(
        StageDef("s1", "Where",
          Map("condition" -> ColumnDef(functions.col("hardener") === 0.0 || functions.col("hardener") === 1.0))),
        StageDef("s2", "Limit",
          Map("size" -> ValueDef(200),
            "inputDf" -> StageOutputDef("s1", "outputDf"))),
        StageDef("s3", "OneHotEncoder",
          Map("inputCol" -> ValueDef("hardener"),
            "outputCol" -> ValueDef("hardener_vec"),
            "inputDf" -> StageOutputDef("s2", "outputDf")))
      ))
  }

  test("run a simple pipeline - successful cases") {
    val pplDef = simplePipeline

    val ppl = requestPipeline("pipeline/create", pplDef)
    assert(ppl.id.nonEmpty)

    val dfIn = getCylinderBands

    val runDef = PipelineRunDef(PipelineDef(ppl.id, Array()),
      Map("s1:inputDf" -> DataframeMessageDef(dfIn.id)),
      Array(StageOutputDef("s3", "outputDf")))

    val runDefWithoutPipelineId = PipelineRunDef(simplePipeline,
      Map("s1:inputDf" -> DataframeMessageDef(dfIn.id)),
      Array(StageOutputDef("s3", "outputDf")))

    Seq(runDef, runDefWithoutPipelineId).map { pplRunDef =>
      val runResult = request[PipelineRunDef, PipelineRunResultDef]("pipeline/run", pplRunDef)
      assert(runResult.results.length === 1)
      assert(runResult.results(0)._2.isInstanceOf[DataframeMessageDef])

      val dfResultId = runResult.results(0)._2.asInstanceOf[DataframeMessageDef].dfId
      val dfResult = requestDf("df/get", dfResultId.toString)

      import io.cebes.server.routes.df.HttpDfJsonProtocol.dataframeRequestFormat

      assert(count(dfResult) > 0)
      assert(dfResult.schema.size === dfIn.schema.size + 1)
      assert(dfResult.schema.contains("hardener_vec"))
    }

    // get multiple outputs
    val runDefMultOutputs = PipelineRunDef(simplePipeline,
      Map("s1:inputDf" -> DataframeMessageDef(dfIn.id)),
      Array(StageOutputDef("s3", "outputDf"), StageOutputDef("s2", "outputDf")))

    val runResult2 = request[PipelineRunDef, PipelineRunResultDef]("pipeline/run",
      runDefMultOutputs)
    assert(runResult2.results.length === 2)
    assert(runResult2.results.forall(_._2.isInstanceOf[DataframeMessageDef]))

    runResult2.results.foreach { case (outputDef, resultPipelineDef) =>
      val dfResultId = resultPipelineDef.asInstanceOf[DataframeMessageDef].dfId
      val dfResult = requestDf("df/get", dfResultId.toString)

      import io.cebes.server.routes.df.HttpDfJsonProtocol.dataframeRequestFormat
      assert(count(dfResult) > 0)

      outputDef.stageName match {
        case "s2" =>
          assert(dfResult.schema.size === dfIn.schema.size)
          assert(!dfResult.schema.contains("hardener_vec"))
        case "s3" =>
          assert(dfResult.schema.size === dfIn.schema.size + 1)
          assert(dfResult.schema.contains("hardener_vec"))
      }
    }
  }

  test("run pipeline - fail cases") {
    val dfIn = getCylinderBands

    // no input
    val runDefNoInputs = PipelineRunDef(simplePipeline, Map(), Array(StageOutputDef("s3", "outputDf")))
    val ex1 = intercept[ServerException] {
      request[PipelineRunDef, PipelineRunResultDef]("pipeline/run", runDefNoInputs)
    }
    assert(ex1.message.contains("Where(name=s1): Input slot inputDf is undefined"))

    val runDefWrongOutput = PipelineRunDef(simplePipeline,
      Map("s1:inputDf" -> DataframeMessageDef(dfIn.id)),
      Array(StageOutputDef("s3", "wrongOutputName")))
    val ex2 = intercept[ServerException] {
      request[PipelineRunDef, PipelineRunResultDef]("pipeline/run", runDefWrongOutput)
    }
    assert(ex2.message.contains("Invalid slot descriptor s3:wrongOutputName in the output list"))
  }

  test("pipeline with drop: array[string] input slot") {
    val pplDef = PipelineDef(None,
      Array(StageDef("s1", "Drop", Map("colNames" -> ValueDef(Array[String]("hardener", "wax"))))))

    val dfIn = getCylinderBands

    val runDef = PipelineRunDef(pplDef,
      Map("s1:inputDf" -> DataframeMessageDef(dfIn.id)),
      Array(StageOutputDef("s1", "outputDf")))

    val runResult = request[PipelineRunDef, PipelineRunResultDef]("pipeline/run", runDef)
    assert(runResult.results.length === 1)
    assert(runResult.results(0)._2.isInstanceOf[DataframeMessageDef])

    val dfResultId = runResult.results(0)._2.asInstanceOf[DataframeMessageDef].dfId
    val dfResult = requestDf("df/get", dfResultId.toString)

    import io.cebes.server.routes.df.HttpDfJsonProtocol.dataframeRequestFormat

    assert(count(dfResult) > 0)
    assert(dfResult.schema.size === dfIn.schema.size - 2)
    assert(!dfResult.schema.contains("hardener"))
    assert(!dfResult.schema.contains("wax"))
  }

  test("pipeline with drop: array[string] input slot - with placeholders") {
    val pplDef = PipelineDef(None,
      Array(
        StageDef("s0", "ValuePlaceholder", Map("inputVal" -> ValueDef(Array[String]("hardener", "wax")))),
        StageDef("df", "DataframePlaceholder", Map()),
        StageDef("s1", "Drop", Map("colNames" -> StageOutputDef("s0", "outputVal"),
          "inputDf" -> StageOutputDef("df", "outputVal")))
      ))

    val dfIn = getCylinderBands

    // missing the dataframe placeholder
    val ex1 = intercept[ServerException] {
      val runDef1 = PipelineRunDef(pplDef, Map(), Array(StageOutputDef("s1", "outputDf")))
      request[PipelineRunDef, PipelineRunResultDef]("pipeline/run", runDef1)
    }
    assert(ex1.message.contains("DataframePlaceholder(name=df): Input slot inputVal is undefined"))

    // with the dataframe placeholder
    val runDef2 = PipelineRunDef(pplDef, Map("df:inputVal" -> DataframeMessageDef(dfIn.id)),
      Array(StageOutputDef("s1", "outputDf")))
    val runResult2 = request[PipelineRunDef, PipelineRunResultDef]("pipeline/run", runDef2)
    assert(runResult2.results.length === 1)
    assert(runResult2.results(0)._2.isInstanceOf[DataframeMessageDef])
    val dfResultId = runResult2.results(0)._2.asInstanceOf[DataframeMessageDef].dfId
    val dfResult = requestDf("df/get", dfResultId.toString)
    import io.cebes.server.routes.df.HttpDfJsonProtocol.dataframeRequestFormat
    assert(count(dfResult) > 0)
    assert(dfResult.schema.size === dfIn.schema.size - 2)
    assert(!dfResult.schema.contains("hardener"))
    assert(!dfResult.schema.contains("wax"))

    // overwrite the value placeholder with feeds
    val runDef3 = PipelineRunDef(pplDef, Map("df:inputVal" -> DataframeMessageDef(dfIn.id),
    "s0:inputVal" -> ValueDef(Array[String]("hardener", "customer"))),
      Array(StageOutputDef("s1", "outputDf")))
    val runResult3 = request[PipelineRunDef, PipelineRunResultDef]("pipeline/run", runDef3)
    assert(runResult3.results.length === 1)
    assert(runResult3.results(0)._2.isInstanceOf[DataframeMessageDef])
    val dfResultId3 = runResult3.results(0)._2.asInstanceOf[DataframeMessageDef].dfId
    val dfResult3 = requestDf("df/get", dfResultId3.toString)
    assert(count(dfResult3) > 0)
    assert(dfResult3.schema.size === dfIn.schema.size - 2)
    assert(!dfResult3.schema.contains("hardener"))
    assert(!dfResult3.schema.contains("customer"))
    assert(dfResult3.schema.contains("wax"))
  }
}
