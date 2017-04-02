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
import io.cebes.spark.CebesSparkTestInjector
import io.cebes.spark.helpers.{ImplicitExecutor, TestDataHelper, TestPipelineHelper}
import org.scalatest.FunSuite

class VectorAssemblerSuite extends FunSuite with ImplicitExecutor with TestDataHelper with TestPipelineHelper {

  override def beforeAll(): Unit = {
    super.beforeAll()
    createOrReplaceCylinderBands()
  }

  test("simple case") {
    val df = getCylinderBands.limit(100)
    assert(df.numRows > 1)

    val assember = CebesSparkTestInjector.instance[VectorAssembler]
    assember.input(assember.inputCols, Array("press", "viscosity", "caliper"))
    .input(assember.outputCol, "features")
    .input(assember.inputDf, df)

    val result = assember.output(assember.outputDf).getResult()
    assert(result.numRows === df.numRows)
    assert(result.numCols === df.numCols + 1)
    assert(result.columns.last === "features")
    assert(result.schema("features").storageType === StorageTypes.VectorType)

    // cannot override output columns
    assember.input(assember.outputCol, "band_type")
    val ex = intercept[IllegalArgumentException] {
      assember.output(assember.outputDf).getResult()
    }
    assert(ex.getMessage === "VectorAssembler(name=vectorassembler): Output column band_type already exists.")
  }
}
