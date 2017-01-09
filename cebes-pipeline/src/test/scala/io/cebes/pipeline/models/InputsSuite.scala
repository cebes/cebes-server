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

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class InputsTestClass extends Inputs {
  override val _inputs: Seq[Input[PipelineMessage]] = Seq(
    ModelInput("m"), DataframeInput("data"), SampleInput("s")
  )

  def run(): Future[String] = {
    withInputs("m", "s") { (m: ModelMessage, s: SampleMessage) =>
      s"In run(): ${m.getClass.getSimpleName} ${s.getClass.getSimpleName}"
    }
  }
}

class InputsSuite extends FunSuite {

  test("simple case") {
    val c = new InputsTestClass()
    c.input(0, Future(new ModelMessage()))
    val ex1 = intercept[NoSuchElementException] {
      Await.result(c.run(), Duration.Inf)
    }
    assert(ex1.getMessage.startsWith("Input named s is not specified."))

    c.input(2, Future(new SampleMessage()))
    assert(Await.result(c.run(), Duration.Inf) === "In run(): ModelMessage SampleMessage")

    c.input(0, Future(new DataframeMessage()))
    val ex = intercept[IllegalArgumentException] {
      Await.result(c.run(), Duration.Inf)
    }
    assert(ex.getMessage.contains("invalid type for input named m, expected ModelMessage, got DataframeMessage"))
  }
}
