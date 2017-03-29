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
package io.cebes.spark.pipeline.features

import io.cebes.spark.CebesSparkTestInjector
import io.cebes.spark.helpers.{ImplicitExecutor, TestDataHelper, TestPipelineHelper}
import org.scalatest.FunSuite

class StringIndexerSuite extends FunSuite with ImplicitExecutor with TestDataHelper with TestPipelineHelper {

  override def beforeAll(): Unit = {
    super.beforeAll()
    createOrReplaceCylinderBands()
  }

  test("simple case") {
    val indexer = CebesSparkTestInjector.instance[StringIndexer]
    val df = getCylinderBands.limit(200)

    indexer.input(indexer.inputCol, "customer")
      .input(indexer.outputCol, "customer_indexed")
      .input(indexer.inputDf, df)

    val labels = indexer.output(indexer.model).getResult(TEST_WAIT_TIME)
    val resultDf = indexer.output(indexer.outputDf).getResult(TEST_WAIT_TIME)
    assert(resultDf.numRows === 200)
    assert(resultDf.numCols === df.numCols + 1)
    assert(resultDf.columns.last === "customer_indexed")
    assert(labels.length > 1)

    val df2 = getCylinderBands.limit(250)
    val result2 = indexer.input(indexer.inputDf, df2).output(indexer.outputDf).getResult(TEST_WAIT_TIME)
    assert(result2.numRows === 250)
    assert(result2.numCols === df2.numCols + 1)
    assert(result2.columns.last === "customer_indexed")
    assert(labels eq indexer.output(indexer.model).getResult(TEST_WAIT_TIME))
  }
}
