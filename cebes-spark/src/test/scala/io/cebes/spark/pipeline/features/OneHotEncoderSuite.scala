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

import io.cebes.df.functions
import io.cebes.df.types.StorageTypes
import io.cebes.spark.CebesSparkTestInjector
import io.cebes.spark.helpers.{ImplicitExecutor, TestDataHelper, TestPipelineHelper}
import org.scalatest.FunSuite

class OneHotEncoderSuite extends FunSuite with ImplicitExecutor with TestDataHelper with TestPipelineHelper {

  override def beforeAll(): Unit = {
    super.beforeAll()
    createOrReplaceCylinderBands()
  }

  test("simple case") {
    val df = getCylinderBands.where(functions.col("hardener") === 0.0 ||
      functions.col("hardener") === 1.0).limit(200)
    assert(df.numRows > 1)

    val encoder = CebesSparkTestInjector.instance[OneHotEncoder]
    encoder.input(encoder.inputCol, "hardener")
      .input(encoder.outputCol, "hardener_vec")
      .input(encoder.inputDf, df)

    val result = encoder.output(encoder.outputDf).getResult()
    assert(result.numCols === df.numCols + 1)
    assert(result.numRows === df.numRows)
    assert(result.columns.last === "hardener_vec")
    assert(result.schema("hardener_vec").storageType === StorageTypes.VectorType)
  }

  test("with string indexer") {
    val df = getCylinderBands.limit(200)
    assert(df.numRows > 1)
    val indexer = CebesSparkTestInjector.instance[StringIndexer]
    indexer.input(indexer.inputCol, "customer")
    .input(indexer.outputCol, "customer_idx")
    .input(indexer.inputDf, df)
    val encoder = CebesSparkTestInjector.instance[OneHotEncoder]
    encoder.input(encoder.inputCol, "customer_idx")
      .input(encoder.outputCol, "customer_vec")
      .input(encoder.inputDf, indexer.output(indexer.outputDf))

    val result = encoder.output(encoder.outputDf).getResult()
    assert(result.numCols === df.numCols + 2)
    assert(result.numRows === df.numRows)
    assert(result.columns.last === "customer_vec")
    assert(result.schema("customer_vec").storageType === StorageTypes.VectorType)

    val df2 = getCylinderBands.limit(150)
    indexer.input(indexer.inputDf, df2)
    val result2 = encoder.output(encoder.outputDf).getResult()
    assert(result2.numCols === df2.numCols + 2)
    assert(result2.numRows === df2.numRows)
    assert(result2.columns.last === "customer_vec")
    assert(result2.schema("customer_vec").storageType === StorageTypes.VectorType)
  }
}
