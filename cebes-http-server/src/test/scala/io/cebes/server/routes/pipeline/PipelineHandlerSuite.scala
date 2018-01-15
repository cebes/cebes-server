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
import io.cebes.http.client.ServerException
import io.cebes.pipeline.json._
import io.cebes.server.helpers.TestPropertyHelper
import io.cebes.server.routes.AbstractRouteSuite
import io.cebes.server.routes.common.HttpServerJsonProtocol._
import io.cebes.server.routes.common.HttpTagJsonProtocol._
import io.cebes.server.routes.common._
import io.cebes.server.routes.pipeline.HttpPipelineJsonProtocol._
import io.cebes.spark.json.CebesSparkJsonProtocol._
import io.cebes.tag.Tag
import spray.json.DefaultJsonProtocol._

/**
  * Test suite for [[PipelineHandler]]
  */
class PipelineHandlerSuite extends AbstractRouteSuite with TestPropertyHelper {

  private def safeDeleteTag(tag: Tag): Unit = {
    try {
      requestPipeline("pipeline/tagdelete", TagDeleteRequest(tag))
    } catch {
      case _: ServerException =>
    }

  }

  private def addTag(tag: Tag, id: UUID): Unit = requestPipeline("pipeline/tagadd", TagAddRequest(tag, id))

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

  test("query tag information") {
    val pplResult = requestPipeline("pipeline/create", simplePipeline)
    assert(pplResult.id.nonEmpty)
    val pipelineId = pplResult.id.get

    val testTag1 = Tag.fromString("my-tag/abc:v1")
    safeDeleteTag(testTag1)
    addTag(testTag1, pipelineId)

    // it picks up the default
    val r1 = wait(postAsync[TagInfoRequest, TagInfoResponse]("pipeline/taginfo",
      TagInfoRequest("my-tag/abc:v1", None, None)))
    assert(r1.tag === testTag1)
    assert(r1.host.nonEmpty)
    assert(r1.port > 0)
    assert(r1.path === "my-tag/abc")
    assert(r1.version === "v1")

    // lookup with pipeline id
    val r1a = wait(postAsync[TagInfoRequest, TagInfoResponse]("pipeline/taginfo",
      TagInfoRequest(s"$pipelineId", None, None)))
    assert(r1a.tag === testTag1)
    assert(r1a.host.nonEmpty)
    assert(r1a.port > 0)
    assert(r1a.path === "my-tag/abc")
    assert(r1a.version === "v1")

    val r1b = wait(postAsync[TagInfoRequest, TagInfoResponse]("pipeline/taginfo",
        TagInfoRequest("new-tag", None, None)))
    assert(r1b.tag === Tag.fromString("new-tag"))
    assert(r1b.host.nonEmpty)
    assert(r1b.port > 0)
    assert(r1b.path === "new-tag")
    assert(r1b.version === "default")

    // it picks up the default given in the request
    val r2 = wait(postAsync[TagInfoRequest, TagInfoResponse]("pipeline/taginfo",
      TagInfoRequest("my-tag/abc:v1", Some("myserver.com"), Some(22000))))
    assert(r1.tag === testTag1)
    assert(r2.host === "myserver.com")
    assert(r2.port === 22000)
    assert(r2.path === "my-tag/abc")
    assert(r2.version === "v1")
    safeDeleteTag(testTag1)

    val testTag2 = Tag.fromString("myserver.net:22000/my-tag/abc:v1")
    safeDeleteTag(testTag2)
    addTag(testTag2, pipelineId)

    // parses things correctly
    val r3 = wait(postAsync[TagInfoRequest, TagInfoResponse]("pipeline/taginfo",
      TagInfoRequest("myserver.net:22000/my-tag/abc:v1", None, None)))
    assert(r3.tag === testTag2)
    assert(r3.host === "myserver.net")
    assert(r3.port === 22000)
    assert(r3.path === "my-tag/abc")
    assert(r3.version === "v1")

    // parses things correctly, even with defaults provided
    val r4 = wait(postAsync[TagInfoRequest, TagInfoResponse]("pipeline/taginfo",
      TagInfoRequest("myserver.net:22000/my-tag/abc:v1", Some("myserver.com"), Some(15))))
    assert(r4.tag === testTag2)
    assert(r4.host === "myserver.net")
    assert(r4.port === 22000)
    assert(r4.path === "my-tag/abc")
    assert(r4.version === "v1")
    safeDeleteTag(testTag2)
  }

  test("repository login - fail") {
    val ex1 = intercept[ServerException] {
      wait(postAsync[PipelineRepoLoginRequest, PipelineRepoLoginResponse](
        "pipeline/login", PipelineRepoLoginRequest(
          Some("non-exist-host.net"), Some(80),
          "user", "password")))
    }
    assert(ex1.message.contains("non-exist-host.net"))
  }

  test("repository push, pull", RepositoryTestsEnabled) {
    // login
    val loginResult = wait(postAsync[PipelineRepoLoginRequest, PipelineRepoLoginResponse](
      "pipeline/login", PipelineRepoLoginRequest(
        Some(properties.repositoryHost), Some(properties.repositoryPort),
        "user", "password")))
    assert(loginResult.host === properties.repositoryHost)
    assert(loginResult.port === properties.repositoryPort)
    assert(loginResult.token.nonEmpty)

    val authToken = loginResult.token

    // create a new pipeline and tag it
    val testTag = Tag.fromString(s"${properties.repositoryHost}:${properties.repositoryPort}/test-sample-pipeline")
    try {
      requestPipeline("pipeline/tagdelete", TagDeleteRequest(testTag))
    } catch {
      case _: ServerException =>
    }
    val pplResult = requestPipeline("pipeline/create", simplePipeline)
    assert(pplResult.id.nonEmpty)
    val pipelineId = pplResult.id.get
    requestPipeline("pipeline/tagadd", TagAddRequest(testTag, pipelineId))

    // push the pipeline - fail without token
    val ex2 = intercept[ServerException] {
      wait(postAsync[PipelinePushRequest, String]("pipeline/push",
        PipelinePushRequest(testTag, None, None, None)))
    }
    assert(ex2.message.contains("The supplied authentication is not authorized to access this resource"))

    // push the pipeline - using the host and port in the tag
    val pushResult1 = wait(postAsync[PipelinePushRequest, String]("pipeline/push",
      PipelinePushRequest(testTag, None, None, Some(authToken))))
    assert(pushResult1.contains("test-sample-pipeline"))

    // push again with another tag, should fail since it is not created
    val testTag2 = Tag.fromString("test-second-tag")
    try {
      requestPipeline("pipeline/tagdelete", TagDeleteRequest(testTag2))
    } catch {
      case _: ServerException =>
    }

    val ex3 = intercept[ServerException] {
      wait(postAsync[PipelinePushRequest, String]("pipeline/push",
        PipelinePushRequest(testTag2, Some(properties.repositoryHost),
          Some(properties.repositoryPort), Some(authToken))))
    }
    assert(ex3.getMessage.contains("Tag not found"))

    // add tag and push again - use host and port provided
    requestPipeline("pipeline/tagadd", TagAddRequest(testTag2, pipelineId))

    val pushResult2 = wait(postAsync[PipelinePushRequest, String]("pipeline/push",
      PipelinePushRequest(testTag2, Some(properties.repositoryHost),
        Some(properties.repositoryPort), Some(authToken))))
    assert(pushResult2.contains("test-second-tag"))


    // delete tags
    requestPipeline("pipeline/tagdelete", TagDeleteRequest(testTag))
    requestPipeline("pipeline/tagdelete", TagDeleteRequest(testTag2))

    // the test tag is gone
    val ex4 = intercept[ServerException] {
      requestPipeline("pipeline/get", testTag.toString)
    }
    assert(ex4.message.contains(s"Tag not found: ${testTag.toString}"))

    // pull testTag
    val pullResult = wait(postAsync[PipelinePushRequest, PipelineDef]("pipeline/pull",
      PipelinePushRequest(testTag, None, None, Some(authToken))))
    assert(pullResult.id.get === pipelineId)

    // can get using the tag
    val ppl2 = requestPipeline("pipeline/get", testTag.toString)
    assert(ppl2.id.get === pipelineId)

    // delete the tag
    requestPipeline("pipeline/tagdelete", TagDeleteRequest(testTag))
  }

  test("create, tag and untag") {

    try {
      requestPipeline("pipeline/tagdelete", TagDeleteRequest(Tag.fromString("tag1:default")))
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
    assert(ex1.getMessage.startsWith("Tag tag1:default already exists"))

    val ex2 = intercept[ServerException] {
      requestPipeline("pipeline/tagadd", TagAddRequest(Tag.fromString("tag1:default"), pplResult2.id.get))
    }
    assert(ex2.getMessage.startsWith("Tag tag1:default already exists"))

    // get pipeline by tag
    val ppl2 = requestPipeline("pipeline/get", "tag1:default")
    assert(ppl2.id.get === pplResult.id.get)
    assert(ppl2.stages(0) === pplResult.stages(0))

    // get all the tags
    val tags = request[TagsGetRequest, Array[TaggedPipelineResponse]]("pipeline/tags", TagsGetRequest(None, 10))
    assert(tags.nonEmpty)
    assert(tags.exists(_.tag.toString === "tag1:default"))

    val tags1 = request[TagsGetRequest, Array[TaggedPipelineResponse]]("pipeline/tags",
      TagsGetRequest(Some("randomstuff???"), 10))
    assert(tags1.isEmpty)

    // delete tag
    requestPipeline("pipeline/tagdelete", TagDeleteRequest(Tag.fromString("tag1:default")))

    val tags2 = request[TagsGetRequest, Array[TaggedPipelineResponse]]("pipeline/tags", TagsGetRequest(None, 10))
    assert(!tags2.exists(_.tag.toString === "tag1:default"))

    // cannot get the tag again
    val ex3 = intercept[ServerException](requestPipeline("pipeline/get", "tag1:default"))
    assert(ex3.getMessage.startsWith("Tag not found: tag1:default"))

    // cannot delete non-existed tag
    val ex4 = intercept[ServerException](requestPipeline("pipeline/tagdelete",
      TagDeleteRequest(Tag.fromString("tag1:default"))))
    assert(ex4.getMessage.startsWith("Tag not found: tag1:default"))

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
