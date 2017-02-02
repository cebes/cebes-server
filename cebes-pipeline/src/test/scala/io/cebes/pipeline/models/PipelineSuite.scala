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
package io.cebes.pipeline.models

import java.util.concurrent.TimeUnit

import io.cebes.pipeline.factory.{PipelineFactory, StageFactory}
import io.cebes.pipeline.protos.message.{PipelineMessageDef, StageOutputDef}
import io.cebes.pipeline.protos.pipeline.PipelineDef
import io.cebes.pipeline.protos.stage.StageDef
import io.cebes.pipeline.protos.value.{ArrayDef, ScalarDef, ValueDef}
import org.scalatest.FunSuite

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

class PipelineSuite extends FunSuite {

  implicit val ec: ExecutionContext = ExecutionContext.global

  private lazy val pipelineFactory = {
    val stageFactory = new StageFactory()
    new PipelineFactory(stageFactory)
  }

  test("SlotDescriptor") {
    val slot1 = SlotDescriptor("stage2:out3")
    assert(slot1.parent === "stage2")
    assert(slot1.name === "out3")

    val ex = intercept[IllegalArgumentException] {
      SlotDescriptor("stage_2:out4:out5")
    }
    assert(ex.getMessage === "Invalid slot descriptor: stage_2:out4:out5")
  }

  test("pipeline factory simple cases") {
    val pipelineDef1 = PipelineDef().withStage(Seq(
      StageDef().withName("stage1").withStage("StageFoo"),
      StageDef().withName("stage2").withStage("StageTwoInputs")
        .addAllInput(Seq(
          "m" -> PipelineMessageDef().withValue(ValueDef().withScalar(ScalarDef().withStringVal("my input value")))
        ))
    ))
    val ppl1 = pipelineFactory.create(pipelineDef1)
    assert(ppl1.stages.size === 2)
    assert(ppl1.stages("stage2").isInstanceOf[StageTwoInputs])
    val stage2 = ppl1.stages("stage2").asInstanceOf[StageTwoInputs]
    assert(stage2.input(stage2.m).get === "my input value")

    val ex1 = intercept[NoSuchElementException] {
      ppl1.run(Seq("stage1:out", "stage2:arrOut"), Map())
    }
    assert(ex1.getMessage === "StageFoo(name=stage1): Input slot strIn is undefined")

    val ex2 = intercept[NoSuchElementException] {
      ppl1.run(Seq("stage1:out", "stage2:arrOut"), Map(
        "stage1:strIn" -> PipelineMessageDef().withValue(
          ValueDef().withScalar(ScalarDef().withStringVal("my input value for stage 1")))
      ))
    }
    assert(ex2.getMessage === "StageTwoInputs(name=stage2): Input slot valIn is undefined")

    // only get output of stage1 is ok, although we stage2 isn't fed yet
    val result1 = ppl1.run(Seq("stage1:out"), Map(
      "stage1:strIn" -> PipelineMessageDef().withValue(
        ValueDef().withScalar(ScalarDef().withStringVal("my input value for stage 1")))
    ))
    assert(result1.size === 1)
    assert(result1("stage1:out").msg.isValue)
    assert(result1("stage1:out").getValue.value.isArray)
    assert(result1("stage1:out").getValue.getArray.element.length === 1)
    assert(result1("stage1:out").getValue.getArray.element.head.value.isScalar)
    assert(result1("stage1:out").getValue.getArray.element.head.getScalar.value.isInt32Val)
    assert(result1("stage1:out").getValue.getArray.element.head.getScalar.getInt32Val === 2000)

    // feed the wrong data type
    val ex3 = intercept[IllegalArgumentException] {
      ppl1.run(Seq("stage1:out"), Map(
        "stage1:strIn" -> PipelineMessageDef().withValue(
          ValueDef().withScalar(ScalarDef().withFloatVal(205.4f)))
      ))
    }
    assert(ex3.getMessage === "requirement failed: StageFoo(name=stage1): Input slot strIn needs " +
      "type java.lang.String, but got value 205.4 of type java.lang.Float")

    // feed the output
    val ex4 = intercept[IllegalArgumentException] {
      ppl1.run(Seq("stage1:out"), Map(
        "stage1:strIn" -> PipelineMessageDef().withValue(
          ValueDef().withScalar(ScalarDef().withStringVal("my string"))),
        "stage1:out" -> PipelineMessageDef().withValue(
          ValueDef().withArray(ArrayDef(Seq(ValueDef().withScalar(ScalarDef().withInt32Val(205))))))
      ))
    }
    assert(ex4.getMessage === "Input name out not found in stage StageFoo(name=stage1)")

    // feed stage1's output into stage2's input
    val result2 = ppl1.run(Seq("stage1:out", "stage2:arrOut"), Map(
      "stage1:strIn" -> PipelineMessageDef().withValue(
        ValueDef().withScalar(ScalarDef().withStringVal("my input value for stage 1"))),
      "stage2:valIn" -> PipelineMessageDef().withStageOutput(
        StageOutputDef("stage1", "out")
      )
    ))
    assert(result2.size === 2)
    assert(result2("stage1:out").msg.isValue)
    assert(result2("stage2:arrOut").msg.isValue)
    assert(result2("stage2:arrOut").toString ===
      """value {
        |  array {
        |    element {
        |      scalar {
        |        float_val: 1.0
        |      }
        |    }
        |    element {
        |      scalar {
        |        float_val: 2.0
        |      }
        |    }
        |  }
        |}
        |""".stripMargin)
  }

  test("slightly complicated pipeline") {
    val pipelineDef1 = PipelineDef().withStage(Seq(
      StageDef().withName("stage1").withStage("StageFoo").addAllInput(Seq(
        "strIn" -> PipelineMessageDef().withStageOutput(StageOutputDef("stage3", "m"))
      )),
      StageDef().withName("stage2").withStage("StageTwoInputs").addAllInput(Seq(
        "valIn" -> PipelineMessageDef().withStageOutput(StageOutputDef("stage1", "out")),
        "m" -> PipelineMessageDef().withStageOutput(StageOutputDef("stage3", "m"))
      )),
      StageDef().withName("stage3").withStage("StageBar")
    ))
    val ppl1 = pipelineFactory.create(pipelineDef1)
    val ex1 = intercept[NoSuchElementException] {
      ppl1.run(Seq("stage3:m", "stage2:arrOut"), Map())
    }
    assert(ex1.getMessage === "StageBar(name=stage3): Input slot strIn is undefined")

    val result1 = ppl1.run(Seq("stage3:m", "stage2:arrOut"), Map(
      "stage3:strIn" -> PipelineMessageDef().withValue(
        ValueDef().withScalar(ScalarDef().withStringVal("my input value for stage 3")))
    ))
    assert(result1.size === 2)
  }

  test("loopy pipeline") {
    val pipelineDef1 = PipelineDef().withStage(Seq(
      StageDef().withName("stage1").withStage("StageFoo").addAllInput(Seq(
        "strIn" -> PipelineMessageDef().withStageOutput(StageOutputDef("stage3", "m"))
      )),
      StageDef().withName("stage2").withStage("StageBar").addAllInput(Seq(
        "strIn" -> PipelineMessageDef().withStageOutput(StageOutputDef("stage3", "m"))
      )),
      StageDef().withName("stage3").withStage("StageBar").addAllInput(Seq(
        "strIn" -> PipelineMessageDef().withStageOutput(StageOutputDef("stage2", "m"))
      ))
    ))
    val ppl1 = pipelineFactory.create(pipelineDef1)

    val ex = intercept[IllegalArgumentException] {
      ppl1.run(Seq("stage1:out"), Map())
    }
    assert(ex.getMessage.contains("There is a loop in the pipeline"))
  }

  test("wrong input array type") {
    val pipelineDef1 = PipelineDef().withStage(Seq(
      StageDef().withName("stage1").withStage("StageTwoInputs").addAllInput(Seq(
        "m" -> PipelineMessageDef().withValue(ValueDef().withScalar(ScalarDef().withStringVal("my input value"))),
        "valIn" -> PipelineMessageDef().withValue(
          ValueDef().withArray(ArrayDef(Seq(ValueDef().withScalar(ScalarDef().withFloatVal(10.3f))))))
      ))
    ))
    val ex1 = intercept[IllegalArgumentException] {
      pipelineFactory.create(pipelineDef1)
    }
    assert(ex1.getMessage.contains("StageTwoInputs(name=stage1): Input slot valIn needs " +
      "type [I, but got value [F"))

    val pipelineDef2 = PipelineDef().withStage(Seq(
      StageDef().withName("stage1").withStage("StageTwoInputs").addAllInput(Seq(
        "m" -> PipelineMessageDef().withValue(ValueDef().withScalar(ScalarDef().withStringVal("my input value")))
      ))
    ))

    val ppl2 = pipelineFactory.create(pipelineDef2)

    val ex2 = intercept[IllegalArgumentException] {
      ppl2.run(Seq("stage1:arrOut"), Map(
        "stage1:valIn" -> PipelineMessageDef().withValue(
          ValueDef().withArray(ArrayDef(Seq(ValueDef().withScalar(ScalarDef().withFloatVal(10.3f))))))
      ))
    }
    assert(ex2.getMessage.contains("StageTwoInputs(name=stage1): Input slot valIn needs type [I, but got value [F"))

    val result = ppl2.run(Seq("stage1:arrOut"), Map(
      "stage1:valIn" -> PipelineMessageDef().withValue(
        ValueDef().withArray(ArrayDef(Seq(ValueDef().withScalar(ScalarDef().withInt32Val(10))))))
    ))
    assert(result.size === 1)
    assert(result("stage1:arrOut").toString ===
      """value {
        |  array {
        |    element {
        |      scalar {
        |        float_val: 1.0
        |      }
        |    }
        |    element {
        |      scalar {
        |        float_val: 2.0
        |      }
        |    }
        |  }
        |}
        |""".stripMargin)
  }

  test("updated in upstream") {
    val s1 = new StageFoo().setName("stage1")
    s1.input(s1.strIn, "input1")
    val s2 = new StageTwoInputs().setName("stage2")
    s2.input(s2.valIn, s1.output(s1.out)).input(s2.m, "abcd")

    val waitDuration = Duration(10, TimeUnit.SECONDS)
    val f1 = s1.output(s1.out).getFuture
    val result1 = Await.result(s2.output(s2.arrOut).getFuture, waitDuration)
    assert(f1 eq s1.output(s1.out).getFuture)
    assert(result1 eq Await.result(s2.output(s2.arrOut).getFuture, waitDuration))
    assert(f1 eq s1.output(s1.out).getFuture)
    assert(result1 eq Await.result(s2.output(s2.arrOut).getFuture, waitDuration))
    assert(f1 eq s1.output(s1.out).getFuture)
    assert(result1 eq Await.result(s2.output(s2.arrOut).getFuture, waitDuration))

    // change input of stage1
    s1.input(s1.strIn, "new input")
    assert(f1 ne s1.output(s1.out).getFuture)
    val result2 = Await.result(s2.output(s2.arrOut).getFuture, waitDuration)
    assert(result1 ne result2)
    assert(result2 eq Await.result(s2.output(s2.arrOut).getFuture, waitDuration))
    assert(result2 eq Await.result(s2.output(s2.arrOut).getFuture, waitDuration))
  }

  test("updated in upstream with non-deterministic stage") {
    val s1 = new StageFooNonDeterministic().setName("stage1")
    s1.input(s1.strIn, "input1")
    val s2 = new StageTwoInputs().setName("stage2")
    s2.input(s2.valIn, s1.output(s1.out)).input(s2.m, "abcd")

    val waitDuration = Duration(10, TimeUnit.SECONDS)
    val f1 = s1.output(s1.out).getFuture
    val result1 = Await.result(s2.output(s2.arrOut).getFuture, waitDuration)
    assert(f1 ne s1.output(s1.out).getFuture)
    assert(result1 ne Await.result(s2.output(s2.arrOut).getFuture, waitDuration))
    assert(s1.output(s1.out).getFuture ne s1.output(s1.out).getFuture)
    assert(Await.result(s2.output(s2.arrOut).getFuture, waitDuration) ne
      Await.result(s2.output(s2.arrOut).getFuture, waitDuration))

    // change input of stage1
    s1.input(s1.strIn, "new input")
    assert(f1 ne s1.output(s1.out).getFuture)
    assert(s1.output(s1.out).getFuture ne s1.output(s1.out).getFuture)

    val result2 = Await.result(s2.output(s2.arrOut).getFuture, waitDuration)
    assert(result1 ne result2)
    assert(result2 ne Await.result(s2.output(s2.arrOut).getFuture, waitDuration))
    assert(result2 ne Await.result(s2.output(s2.arrOut).getFuture, waitDuration))
  }
}
