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

import org.scalatest.FunSuite

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionException}

class StageSuite extends FunSuite {

  implicit val ec: ExecutionContext = ExecutionContext.global

  test("bad input") {
    val s = new StageFoo().setName("foo")
    val ex = intercept[NoSuchElementException] {
      Await.result(s.output(s.out).getFuture, Duration.Inf)
    }
    assert(ex.getMessage.contains("Input slot strIn is undefined"))

    s.input(s.strIn, "input string")
    val out1 = Await.result(s.output(s.out).getFuture, Duration.Inf)
    assert(out1.isInstanceOf[Array[Int]])
  }

  test("caching") {
    val s = new StageFoo().setName("foo")
    s.input(s.strIn, "input string")
    val out1 = Await.result(s.output(s.out).getFuture, Duration.Inf)
    assert(out1.isInstanceOf[Array[Int]])

    // calling output() again (and again and again) will give the same output
    assert(out1 eq Await.result(s.output(s.out).getFuture, Duration.Inf))
    assert(out1 eq Await.result(s.output(s.out).getFuture, Duration.Inf))
    assert(out1 eq Await.result(s.output(s.out).getFuture, Duration.Inf))

    // feed a different input, it will change the result
    s.input(s.strIn, "input string")
    val out2 = Await.result(s.output(s.out).getFuture, Duration.Inf)
    assert(out1 ne out2)
    assert(out2 eq Await.result(s.output(s.out).getFuture, Duration.Inf))
  }

  test("bad output size") {
    val stage1 = new StageBadOutputSize().setName("stage1")

    val ex2 = intercept[IllegalArgumentException] {
      Await.result(stage1.output(stage1.m1).getFuture, Duration.Inf)
    }
    assert(ex2.getMessage === "requirement failed: Stage StageBadOutputSize(name=stage1): " +
      "output doesn't contain result for slot m2")

    val ex3 = intercept[IllegalArgumentException] {
      Await.result(stage1.output(stage1.m2).getFuture, Duration.Inf)
    }
    assert(ex3.getMessage === "requirement failed: Stage StageBadOutputSize(name=stage1): " +
      "output doesn't contain result for slot m2")
  }

  test("bad output type") {
    val stage1 = new StageBadOutputType().setName("stage1")

    val ex2 = intercept[IllegalArgumentException] {
      Await.result(stage1.output(stage1.m).getFuture, Duration.Inf)
    }
    assert(ex2.getMessage === "StageBadOutputType(name=stage1): requirement failed: " +
      "Invalid type at slot m, expected a String, got Integer")
  }

  test("chaining") {
    val s1 = new StageFoo().setName("s1")
    val s2 = new StageTwoInputs().setName("s2")

    s1.input(s1.strIn, "input string")
    s2.input(s2.valIn, s1.output(s1.out))
    val ex1 = intercept[NoSuchElementException] {
      Await.result(s2.output(s2.arrOut).getFuture, Duration.Inf)
    }
    assert(ex1.getMessage.contains("Input slot m is undefined"))

    s2.input(s2.m, "lalala")
    val out = Await.result(s2.output(s2.arrOut).getFuture, Duration.Inf)
    assert(out.isInstanceOf[Array[Float]])
    assert(out === Array(1.0f, 2.0f))
    assert(out eq Await.result(s2.output(s2.arrOut).getFuture, Duration.Inf))

    // change input of source
    s1.input(s1.strIn, "second string")

    val out2 = Await.result(s2.output(s2.arrOut).getFuture, Duration.Inf)
    assert(out2 === Array(1.0f, 2.0f))
    assert(out2 ne out)
  }

  test("typo in slot name") {
    val s = new StageFooTypoSlotName()
    val ex = intercept[IllegalArgumentException] {
      s.hasInput("strIn")
    }
    assert(ex.getMessage.contains("StageFooTypoSlotName: inconsistent slot name: " +
      "variable named strIn, slot named strInlala"))
  }

  test("stateful output without implementation") {
    val s = new StageStatefulOutputDumb()
    s.input(s.valIn, Array[Int](10, 20))

    val ex = intercept[ExecutionException] {
      Await.result(s.output(s.arrOutStateful).getFuture, Duration.Inf)
    }
    assert(ex.getCause.isInstanceOf[NotImplementedError])
    assert(ex.getCause.getMessage.contains("computeStatefulOutputs() not implemented"))
  }

  test("stateful outputs") {
    val s = new StageStatefulOutput()
    s.input(s.valIn, Array[Int](1, 10, 20))

    val o1 = Await.result(s.output(s.arrOutStateless).getFuture, Duration.Inf)
    assert(o1 === Array(1.5f, 3.5f))
    assert(o1 eq Await.result(s.output(s.arrOutStateless).getFuture, Duration.Inf))

    val o2 = Await.result(s.output(s.arrOutStateful).getFuture, Duration.Inf)
    assert(o2 === Array(1.0f, 3.0f))
    assert(o2 eq Await.result(s.output(s.arrOutStateful).getFuture, Duration.Inf))

    // new output
    s.input(s.valIn, Array[Int](4, 5))

    // stateless output will change
    val o3 = Await.result(s.output(s.arrOutStateless).getFuture, Duration.Inf)
    assert(o3 === Array(1.5f, 3.5f))
    assert(o3 ne o1)

    // stateful output doesn't change
    val o4 = Await.result(s.output(s.arrOutStateful).getFuture, Duration.Inf)
    assert(o4 eq o2)

    // clear stateful output, now it should change
    s.clearOutput(s.arrOutStateful)
    val o5 = Await.result(s.output(s.arrOutStateful).getFuture, Duration.Inf)
    assert(o5 === Array(1.0f, 3.0f))
    assert(o5 ne o2)

    // stateless output doesn't change, since we only cleared stateful output
    assert(o3 === Await.result(s.output(s.arrOutStateless).getFuture, Duration.Inf))
  }

  test("stateful output with chaining") {
    val s1 = new StageFoo()
    val s2 = new StageStatefulOutput()

    s1.input(s1.strIn, "my input string")
    s2.input(s2.valIn, s1.output(s1.out))

    val o1 = Await.result(s2.output(s2.arrOutStateful).getFuture, Duration.Inf)
    assert(o1 === Array(1.0f, 3.0f))

    val o2 = Await.result(s2.output(s2.arrOutStateless).getFuture, Duration.Inf)
    assert(o2 === Array(1.5f, 3.5f))

    // change input of the first stage
    s1.input(s1.strIn, "second string")

    //val oo = Await.result(s1.output(s1.out).getFuture, Duration.Inf)

    // stateful output doesn't change
    val o3 = Await.result(s2.output(s2.arrOutStateful).getFuture, Duration.Inf)
    assert(o3 eq o1)

    // stateless output should change
    val o4 = Await.result(s2.output(s2.arrOutStateless).getFuture, Duration.Inf)
    assert(o4 === Array(1.5f, 3.5f))
    assert(o4 ne o2)
  }

  test("stateful output with chaining on stateful output") {
    val s1 = new StageFooStateful()
    val s2 = new StageStatefulOutput()

    s1.input(s1.strIn, "my input string")
    s2.input(s2.valIn, s1.output(s1.out))

    val o1 = Await.result(s2.output(s2.arrOutStateful).getFuture, Duration.Inf)
    assert(o1 === Array(1.0f, 3.0f))

    val o2 = Await.result(s2.output(s2.arrOutStateless).getFuture, Duration.Inf)
    assert(o2 === Array(1.5f, 3.5f))

    // change input of the first stage
    s1.input(s1.strIn, "second string")

    // stateful output doesn't change
    val o3 = Await.result(s2.output(s2.arrOutStateful).getFuture, Duration.Inf)
    assert(o3 eq o1)

    // stateless output also doesn't change, because the stateful output doesn't change
    val o4 = Await.result(s2.output(s2.arrOutStateless).getFuture, Duration.Inf)
    assert(o4 === Array(1.5f, 3.5f))
    assert(o4 eq o2)

    // clear the stateful output
    s1.clearOutput(s1.out)

    // stateful output still doesn't change
    val o5 = Await.result(s2.output(s2.arrOutStateful).getFuture, Duration.Inf)
    assert(o5 eq o1)

    // now the stateless output should change
    val o6 = Await.result(s2.output(s2.arrOutStateless).getFuture, Duration.Inf)
    assert(o6 === Array(1.5f, 3.5f))
    assert(o6 ne o2)
  }

  test("stateful output with chaining on stateful output (which has stateful input)") {
    val s1 = new StageFooStatefulInput()
    val s2 = new StageStatefulOutputWithStatefulInput()

    s1.input(s1.strIn, "my input string")
    s2.input(s2.valIn, s1.output(s1.out))

    val o1 = Await.result(s2.output(s2.arrOutStateful).getFuture, Duration.Inf)
    assert(o1 === Array(1.0f, 3.0f))

    val o2 = Await.result(s2.output(s2.arrOutStateless).getFuture, Duration.Inf)
    assert(o2 === Array(1.5f, 3.5f))

    // change input of the first stage
    s1.input(s1.strIn, "second string")

    // stateful output should change
    val o3 = Await.result(s2.output(s2.arrOutStateful).getFuture, Duration.Inf)
    assert(o3 ne o1)

    // stateless output should change
    val o4 = Await.result(s2.output(s2.arrOutStateless).getFuture, Duration.Inf)
    assert(o4 === Array(1.5f, 3.5f))
    assert(o4 ne o2)

    // stateful output stays the same
    val o3a = s2.output(s2.arrOutStateful).getResult()
    assert(o3a eq o3)

    // clear the stateful output
    s1.clearOutput(s1.out)

    // stateful output should change
    val o5 = Await.result(s2.output(s2.arrOutStateful).getFuture, Duration.Inf)
    assert(o5 ne o1)

    // now the stateless output should change
    val o6 = Await.result(s2.output(s2.arrOutStateless).getFuture, Duration.Inf)
    assert(o6 === Array(1.5f, 3.5f))
    assert(o6 ne o2)
  }

  test("stateful input and output complication") {
    val s = new StageStatefulComplicated()

    s.input(s.inStateful, "stateful in")
      .input(s.inStateless, "stateless in")

    val o1 = s.output(s.outStateful).getResult()
    val o2 = s.output(s.outStateless).getResult()

    assert(o1 eq s.output(s.outStateful).getResult())
    assert(o2 eq s.output(s.outStateless).getResult())

    // change stateless input
    s.input(s.inStateless, "second stateless in")
    // stateful output doesn't change, stateless output changes
    assert(o1 eq s.output(s.outStateful).getResult())
    val o3 = s.output(s.outStateless).getResult()
    assert(o2 ne o3)
    assert(o3 eq s.output(s.outStateless).getResult())

    // change stateful input
    s.input(s.inStateful, "second stateful in")
    // both outputs change
    val o4 = s.output(s.outStateful).getResult()
    val o5 = s.output(s.outStateless).getResult()
    assert(o1 ne o4)
    assert(o3 ne o5)
    assert(o4 eq s.output(s.outStateful).getResult())
    assert(o5 eq s.output(s.outStateless).getResult())
  }
}
