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

import io.cebes.spark.helpers.{ImplicitExecutor, TestDataHelper, TestPipelineHelper}
import org.scalatest.FunSuite

class DropSuite extends FunSuite with ImplicitExecutor with TestDataHelper with TestPipelineHelper {

  override def beforeAll(): Unit = {
    super.beforeAll()
    createOrReplaceCylinderBands()
  }

  test("drop columns") {
    val df = getCylinderBands.limit(50)

    val s = Drop().setName("drop")
    s.input(s.colNames, Array[String]("cylinder_number", "non_existed_column"))
    s.input(s.inputDf, df)
    val df2 = resultDf(s.output(s.outputDf).getFuture)
    assert(df2.numCols + 1 === df.numCols)
    assert(df2.numRows === df.numRows)
    assert(df2.schema.get("cylinder_number").isEmpty)
  }
}
