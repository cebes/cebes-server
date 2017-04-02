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
package io.cebes.spark.pipeline.ml.regression

import io.cebes.df.types.StorageTypes
import io.cebes.spark.CebesSparkTestInjector
import io.cebes.spark.helpers.{ImplicitExecutor, TestDataHelper, TestPipelineHelper}
import io.cebes.spark.pipeline.features.VectorAssembler
import org.scalatest.FunSuite

class LinearRegressionSuite extends FunSuite with ImplicitExecutor with TestDataHelper with TestPipelineHelper {

  override def beforeAll(): Unit = {
    super.beforeAll()
    createOrReplaceCylinderBands()
  }


  test("Linear regression with vector assembler") {
    val df = getCylinderBands.limit(200).na.drop()
    assert(df.numRows > 1)

    val assembler = CebesSparkTestInjector.instance[VectorAssembler]
    assembler.input(assembler.inputCols, Array("viscosity", "proof_cut"))
      .input(assembler.outputCol, "features")
      .input(assembler.inputDf, df)

    val lr = CebesSparkTestInjector.instance[LinearRegression]
    lr.input(lr.featuresCol, "features")
      .input(lr.labelCol, "band_type")
      .input(lr.inputDf, assembler.output(assembler.outputDf))

    val ex0 = intercept[IllegalArgumentException] {
      lr.getModel()
    }
    assert(ex0.getMessage.contains("Column band_type must be of type NumericType but was actually of type StringType."))

    lr.input(lr.labelCol, "caliper")
      .input(lr.predictionCol, "caliper_predict")

    // need to clear the model
    // TODO: this should be fixed, see https://github.com/phvu/cebes-server/issues/88
    lr.clearOutput(lr.model)
    val lrModel = lr.getModel()
    assert(lrModel.isInstanceOf[LinearRegressionModel])

    val dfPredict = lr.output(lr.outputDf).getResult(TEST_WAIT_TIME)
    assert(dfPredict.numRows === df.numRows)
    assert(dfPredict.numCols === df.numCols + 2)
    assert(dfPredict.schema("caliper_predict").storageType === StorageTypes.DoubleType)

    // change input data, but model doesn't change
    val df2 = getCylinderBands.limit(150).na.drop()
    assert(df2.numRows > 1)
    assembler.input(assembler.inputDf, df2)

    val dfPredict2 = lr.output(lr.outputDf).getResult(TEST_WAIT_TIME)
    assert(dfPredict2.numRows === df2.numRows)
    assert(dfPredict2.numCols === df2.numCols + 2)
    assert(dfPredict2.schema("caliper_predict").storageType === StorageTypes.DoubleType)
    assert(lr.getModel() eq lrModel)
  }
}
