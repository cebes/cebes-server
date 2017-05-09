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

import io.cebes.df.types.StorageTypes
import io.cebes.spark.helpers.{ImplicitExecutor, TestDataHelper, TestPipelineHelper}
import org.scalatest.FunSuite

class StringIndexerSuite extends FunSuite with ImplicitExecutor with TestDataHelper with TestPipelineHelper {

  override def beforeAll(): Unit = {
    super.beforeAll()
    createOrReplaceCylinderBands()
  }

  test("simple case") {
    val indexer = getInstance[StringIndexer]
    val df = getCylinderBands.limit(200)

    indexer.input(indexer.inputCol, "customer")
      .input(indexer.outputCol, "customer_indexed")
      .input(indexer.inputDf, df)

    val labels = indexer.output(indexer.model).getResult(TEST_WAIT_TIME)
    val resultDf = indexer.output(indexer.outputDf).getResult(TEST_WAIT_TIME)
    assert(resultDf.numRows === 200)
    assert(resultDf.numCols === df.numCols + 1)
    assert(resultDf.columns.last === "customer_indexed")
    assert(resultDf.schema("customer_indexed").storageType === StorageTypes.DoubleType)
    assert(labels.length > 1)

    val df2 = getCylinderBands.limit(250)
    val result2 = indexer.input(indexer.inputDf, df2).output(indexer.outputDf).getResult(TEST_WAIT_TIME)
    assert(result2.numRows === 250)
    assert(result2.numCols === df2.numCols + 1)
    assert(result2.columns.last === "customer_indexed")
    assert(result2.schema("customer_indexed").storageType === StorageTypes.DoubleType)
    assert(labels eq indexer.output(indexer.model).getResult(TEST_WAIT_TIME))
  }

  test("with indexToString") {
    val indexer = getInstance[StringIndexer]
    val df = getCylinderBands.limit(200)

    indexer.input(indexer.inputCol, "customer")
      .input(indexer.outputCol, "customer_indexed")
      .input(indexer.inputDf, df)

    val labels = indexer.output(indexer.model).getResult(TEST_WAIT_TIME)
    val result = indexer.output(indexer.outputDf).getResult(TEST_WAIT_TIME)
    assert(labels.length > 1)
    assert(result.numCols === df.numCols + 1)
    assert(result.numRows === df.numRows)

    // use ordinal input
    val indexToString = getInstance[IndexToString]
    indexToString.input(indexToString.inputCol, "customer_indexed")
    .input(indexToString.outputCol, "customer_reversed")
    .input(indexToString.labels, labels)
    .input(indexToString.inputDf, result)

    val result2 = indexToString.output(indexToString.outputDf).getResult(TEST_WAIT_TIME)
    assert(result2.numRows === result.numRows)
    assert(result2.numCols === df.numCols + 2)
    val sample = result2.take(20)
    assert(sample("customer").zip(sample("customer_reversed")).forall(p => p._1 === p._2))

    // connect two stages
    val df2 = getCylinderBands.limit(150)
    val indexToString2 = getInstance[IndexToString]
    indexToString2.input(indexToString2.inputCol, "customer_indexed")
      .input(indexToString2.outputCol, "customer_reversed")
      .input(indexToString2.labels, indexer.output(indexer.model))
      .input(indexToString2.inputDf, indexer.output(indexer.outputDf))

    indexer.input(indexer.inputDf, df2)

    val result3 = indexToString2.output(indexToString2.outputDf).getResult(TEST_WAIT_TIME)
    assert(result3.numRows === df2.numRows)
    assert(result3.numCols === df2.numCols + 2)
    val sample2 = result3.take(20)
    assert(sample2("customer").zip(sample2("customer_reversed")).forall(p => p._1 === p._2))
  }
}
