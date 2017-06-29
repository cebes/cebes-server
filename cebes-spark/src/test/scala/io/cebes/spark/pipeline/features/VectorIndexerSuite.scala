/* Copyright 2017 The Cebes Authors. All Rights Reserved.
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
import io.cebes.pipeline.stages.DataframePlaceholder
import io.cebes.spark.helpers.{ImplicitExecutor, TestDataHelper, TestPipelineHelper}
import org.scalatest.FunSuite

class VectorIndexerSuite extends FunSuite with ImplicitExecutor with TestDataHelper with TestPipelineHelper {

  override def beforeAll(): Unit = {
    super.beforeAll()
    createOrReplaceCylinderBands()
  }

  test("simple case") {
    val cols = Array("press", "viscosity", "caliper")
    val df = getCylinderBands.limit(200).na.drop(cols)
    assert(df.numRows > 1)

    val assembler = getInstance[VectorAssembler]
    assembler.input(assembler.inputCols, cols)
      .input(assembler.outputCol, "features")
      .input(assembler.inputDf, df)

    val indexer = getInstance[VectorIndexer]
    indexer.input(indexer.inputCol, "features")
      .input(indexer.outputCol, "features_indexed")
      .input(indexer.maxCategories, 10)
      .input(indexer.inputDf, assembler.output(assembler.outputDf))

    val result = indexer.output(indexer.outputDf).getResult()
    assert(result.numRows === df.numRows)
    assert(result.numCols === df.numCols + 2)
    assert(result.columns.last === "features_indexed")
    assert(result.schema("features").storageType === StorageTypes.VectorType)
  }

  test("with placeholder") {
    val cols = Array("press", "viscosity", "caliper")
    val df = getCylinderBands.limit(200).na.drop(cols)
    assert(df.numRows > 1)

    val placeholder = getInstance[DataframePlaceholder]

    val assembler = getInstance[VectorAssembler]
    assembler.input(assembler.inputCols, cols)
      .input(assembler.outputCol, "features")
      .input(assembler.inputDf, placeholder.output(placeholder.outputVal))

    val indexer = getInstance[VectorIndexer]
    indexer.input(indexer.inputCol, "features")
      .input(indexer.outputCol, "features_indexed")
      .input(indexer.maxCategories, 10)
      .input(indexer.inputDf, assembler.output(assembler.outputDf))

    // placeholder isn't filled
    val ex = intercept[NoSuchElementException] {
      indexer.output(indexer.outputDf).getResult()
    }
    assert(ex.getMessage.contains("Input slot inputVal is undefined"))

    // fill the placeholder
    placeholder.input(placeholder.inputVal, df)
    val result = indexer.output(indexer.outputDf).getResult()
    assert(result.numRows === df.numRows)
    assert(result.numCols === df.numCols + 2)
    assert(result.columns.last === "features_indexed")
    assert(result.schema("features").storageType === StorageTypes.VectorType)
  }
}
