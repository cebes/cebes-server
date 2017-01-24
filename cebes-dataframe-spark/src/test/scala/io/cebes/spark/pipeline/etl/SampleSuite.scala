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
package io.cebes.spark.pipeline.etl

import io.cebes.pipeline.models.DataframeMessage
import io.cebes.spark.helpers.{ImplicitExecutor, TestDataHelper, TestPipelineHelper}
import org.scalatest.FunSuite

import scala.concurrent.Future

class SampleSuite extends FunSuite with ImplicitExecutor with TestDataHelper with TestPipelineHelper {

  override def beforeAll(): Unit = {
    super.beforeAll()
    createOrReplaceCylinderBands()
  }

  test("sample") {
    val df = getCylinderBands

    val s = Sample()
    s.set(s.withReplacement, true)
    s.set(s.fraction, 0.2)
    s.set(s.seed, 128L)
    s.input(0, Future(DataframeMessage(df)))
    val df2 = resultDf(s.output(0))
    assert(df2.numCols === df.numCols)
    assert(df2.numRows < df.numRows)
  }
}
