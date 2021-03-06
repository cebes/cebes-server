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

import io.cebes.pipeline.factory.PipelineFactory
import io.cebes.pipeline.inject.PipelineTestInjector
import io.cebes.pipeline.json.{PipelineDef, StageDef, StageOutputDef, ValueDef}
import org.scalatest.FunSuite

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Awaitable, ExecutionContext}

class PipelineSuite extends FunSuite {

  implicit val ec: ExecutionContext = ExecutionContext.global

  private lazy val pipelineExporter = PipelineTestInjector.instance[PipelineFactory]

  private def wait[T](what: Awaitable[T]): T = Await.result(what, Duration(2, TimeUnit.MINUTES))

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
    val pipelineDef1 = PipelineDef(None, Array(
      StageDef("stage1", "StageFoo"),
      StageDef("stage2", "StageTwoInputs", Map("m" -> ValueDef("my input value")))
    ))
    val ppl1 = pipelineExporter.imports(pipelineDef1, None)
    assert(ppl1.stages.size === 2)
    assert(ppl1.stages("stage2").isInstanceOf[StageTwoInputs])
    val stage2 = ppl1.stages("stage2").asInstanceOf[StageTwoInputs]
    assert(stage2.input(stage2.m).get === "my input value")

    val ex1 = intercept[NoSuchElementException] {
      ppl1.run(Seq(SlotDescriptor("stage1", "out"), SlotDescriptor("stage2", "arrOut")), Map())
    }
    assert(ex1.getMessage === "StageFoo(name=stage1): Input slot strIn is undefined")

    val ex2 = intercept[NoSuchElementException] {
      ppl1.run(Seq(SlotDescriptor("stage1", "out"), SlotDescriptor("stage2", "arrOut")),
        Map(SlotDescriptor("stage1:strIn") -> "my input value for stage 1"))
    }
    assert(ex2.getMessage === "StageTwoInputs(name=stage2): Input slot valIn is undefined")

    // only get output of stage1 is ok, although stage2 isn't fed yet
    val result1 = wait(ppl1.run(Seq(SlotDescriptor("stage1", "out")), Map(
      SlotDescriptor("stage1:strIn") -> "my input value for stage 1")))
    assert(result1.size === 1)
    assert(result1(SlotDescriptor("stage1:out")).isInstanceOf[Array[_]])
    val outputArr = result1(SlotDescriptor("stage1:out")).asInstanceOf[Array[_]]
    assert(outputArr.length === 1)
    assert(outputArr(0).isInstanceOf[Int])
    assert(outputArr(0).asInstanceOf[Int] === 2000)

    // feed the wrong data type
    val ex3 = intercept[IllegalArgumentException] {
      ppl1.run(Seq(SlotDescriptor("stage1:out")),
        Map(SlotDescriptor("stage1:strIn") -> 205.4f))
    }
    assert(ex3.getMessage === "StageFoo(name=stage1): requirement failed: Invalid type at slot strIn, " +
      "expected a String, got Float")

    // feed the output
    val ex4 = intercept[IllegalArgumentException] {
      ppl1.run(Seq(SlotDescriptor("stage1:out")), Map(
        SlotDescriptor("stage1:strIn") -> "my string",
        SlotDescriptor("stage1:out") -> Array(205)))
    }
    assert(ex4.getMessage === "requirement failed: Invalid slot descriptor stage1:out in feeds")

    // feed stage1's output into stage2's input
    val result2 = wait(ppl1.run(Seq(SlotDescriptor("stage1:out"), SlotDescriptor("stage2:arrOut")), Map(
      SlotDescriptor("stage1:strIn") -> "my input value for stage 1",
      SlotDescriptor("stage2:valIn") -> SlotDescriptor("stage1", "out"))))
    assert(result2.size === 2)
    assert(result2(SlotDescriptor("stage1:out")).isInstanceOf[Array[_]])
    assert(result2(SlotDescriptor("stage2:arrOut")).isInstanceOf[Array[_]])
    val arr = result2(SlotDescriptor("stage2:arrOut")).asInstanceOf[Array[_]]
    assert(arr.length === 2)
    assert(arr === Array[Float](1.0f, 2.0f))
  }

  test("slightly complicated pipeline") {
    val pipelineDef1 = PipelineDef(None, Array(
      StageDef("stage1", "StageFoo", Map("strIn" -> StageOutputDef("stage3", "m"))),
      StageDef("stage2", "StageTwoInputs", Map(
        "valIn" -> StageOutputDef("stage1", "out"),
        "m" -> StageOutputDef("stage3", "m"))),
      StageDef("stage3", "StageBar")
    ))
    val ppl1 = pipelineExporter.imports(pipelineDef1, None)
    val ex1 = intercept[NoSuchElementException] {
      ppl1.run(Seq(SlotDescriptor("stage3:m"), SlotDescriptor("stage2:arrOut")))
    }
    assert(ex1.getMessage === "StageBar(name=stage3): Input slot strIn is undefined")

    val result1 = wait(ppl1.run(Seq(SlotDescriptor("stage3:m"), SlotDescriptor("stage2:arrOut")),
      Map(SlotDescriptor("stage3:strIn") -> "my input value for stage 3")))
    assert(result1.size === 2)
  }

  test("loopy pipeline") {
    val pipelineDef1 = PipelineDef(None, Array(
      StageDef("stage1", "StageFoo", Map("strIn" -> StageOutputDef("stage3", "m"))),
      StageDef("stage2", "StageBar", Map("strIn" -> StageOutputDef("stage3", "m"))),
      StageDef("stage3", "StageBar", Map("strIn" -> StageOutputDef("stage2", "m")))))
    val ppl1 = pipelineExporter.imports(pipelineDef1, None)

    val ex = intercept[IllegalArgumentException] {
      ppl1.run(Seq(SlotDescriptor("stage1:out")))
    }
    assert(ex.getMessage.contains("There is a loop in the pipeline"))
  }

  test("wrong input array type") {
    val pipelineDef1 = PipelineDef(None, Array(
      StageDef("stage1", "StageTwoInputs", Map(
        "m" -> ValueDef("my input value"),
        "valIn" -> ValueDef(Array(10.3f))))))
    val ex1 = intercept[IllegalArgumentException] {
      pipelineExporter.imports(pipelineDef1, None)
    }
    assert(ex1.getMessage.contains("StageTwoInputs(name=stage1): requirement failed: " +
      "Invalid type at slot valIn, expected a int[], got float[]"))

    val pipelineDef2 = PipelineDef(None, Array(
      StageDef("stage1", "StageTwoInputs", Map(
        "m" -> ValueDef("my input value")))))

    val ppl2 = pipelineExporter.imports(pipelineDef2, None)

    val ex2 = intercept[IllegalArgumentException] {
      ppl2.run(Seq(SlotDescriptor("stage1:arrOut")), Map(SlotDescriptor("stage1:valIn") -> Array(10.3f)))
    }
    assert(ex2.getMessage.contains("StageTwoInputs(name=stage1): requirement failed: " +
      "Invalid type at slot valIn, expected a int[], got float[]"))

    val result = wait(ppl2.run(Seq(SlotDescriptor("stage1:arrOut")),
      Map(SlotDescriptor("stage1:valIn") -> Array[Int](10))))

    assert(result.size === 1)
    val arr = result(SlotDescriptor("stage1:arrOut")).asInstanceOf[Array[_]]
    assert(arr === Array[Float](1.0f, 2.0f))
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
}
