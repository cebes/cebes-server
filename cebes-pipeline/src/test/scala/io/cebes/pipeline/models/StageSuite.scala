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
import scala.concurrent.{Await, ExecutionContext}

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
  }

  test("typo in slot name") {
    val s = new StageFooTypoSlotName()
    val ex = intercept[IllegalArgumentException] {
      s.hasInput("strIn")
    }
    assert(ex.getMessage.contains("StageFooTypoSlotName: inconsistent slot name: " +
      "variable named strIn, slot named strInlala"))
  }
}

