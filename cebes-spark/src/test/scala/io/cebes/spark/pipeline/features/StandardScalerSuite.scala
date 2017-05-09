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

class StandardScalerSuite extends FunSuite with ImplicitExecutor with TestDataHelper with TestPipelineHelper {

  override def beforeAll(): Unit = {
    super.beforeAll()
    createOrReplaceCylinderBands()
  }

  test("simple case") {
    val cols = Array("press", "viscosity", "caliper")
    val df = getCylinderBands.limit(200).na.drop(cols)
    assert(df.numRows > 10)

    val assembler = getInstance[VectorAssembler]
    assembler.input(assembler.inputCols, cols)
      .input(assembler.outputCol, "features")
      .input(assembler.inputDf, df)

    val scaler = getInstance[StandardScaler]
    scaler.input(scaler.inputCol, "features")
      .input(scaler.outputCol, "features_scaled")
      .input(scaler.inputDf, assembler.output(assembler.outputDf))

    for (withMean <- Seq(true, false)) {
      for (withStd <- Seq(true, false)) {
        scaler.input(scaler.withMean, withMean).input(scaler.withStd, withStd)

        val result = scaler.output(scaler.outputDf).getResult(TEST_WAIT_TIME)
        assert(result.numRows === df.numRows)
        assert(result.numCols === df.numCols + 2)
        assert(result.schema.last.name === "features_scaled")
        assert(result.schema.last.storageType === StorageTypes.VectorType)
      }
    }
  }
}
