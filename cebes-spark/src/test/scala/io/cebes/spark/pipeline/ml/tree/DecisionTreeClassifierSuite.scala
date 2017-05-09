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
package io.cebes.spark.pipeline.ml.tree

import io.cebes.df.types.StorageTypes
import io.cebes.spark.helpers.{ImplicitExecutor, TestDataHelper, TestPipelineHelper}
import io.cebes.spark.pipeline.features.{StringIndexer, VectorAssembler}
import org.scalatest.FunSuite

class DecisionTreeClassifierSuite extends FunSuite with ImplicitExecutor with TestDataHelper with TestPipelineHelper {

  override def beforeAll(): Unit = {
    super.beforeAll()
    createOrReplaceCylinderBands()
  }

  test("Decision tree classifier with vector assembler") {
    val df = getCylinderBands.limit(200).na.drop()
    assert(df.numRows > 1)

    val assembler = getInstance[VectorAssembler]
    assembler.input(assembler.inputCols, Array("viscosity", "proof_cut"))
      .input(assembler.outputCol, "features")
      .input(assembler.inputDf, df)

    val stringIndexer = getInstance[StringIndexer]
    stringIndexer.input(stringIndexer.inputCol, "band_type")
      .input(stringIndexer.outputCol, "band_type_numeric")
      .input(stringIndexer.inputDf, assembler.output(assembler.outputDf))

    val dtc = getInstance[DecisionTreeClassifier]
    dtc.input(dtc.featuresCol, "features")
      .input(dtc.labelCol, "band_type")
      .input(dtc.inputDf, assembler.output(assembler.outputDf))

    val ex0 = intercept[IllegalArgumentException] {
      dtc.getModel()
    }
    assert(ex0.getMessage.contains("Column band_type must be of type NumericType but was actually of type StringType."))

    dtc.input(dtc.labelCol, "band_type_numeric")
      .input(dtc.predictionCol, "band_type_predict")
      .input(dtc.inputDf, stringIndexer.output(stringIndexer.outputDf))

    val dtcModel = dtc.getModel()
    assert(dtcModel.isInstanceOf[DecisionTreeClassifierModel])

    val dfPredict = dtc.output(dtc.outputDf).getResult(TEST_WAIT_TIME)
    assert(dfPredict.numRows === df.numRows)
    assert(dfPredict.numCols === df.numCols + 5)
    assert(dfPredict.schema("band_type_predict").storageType === StorageTypes.DoubleType)

    // change input data, but model doesn't change
    val df2 = getCylinderBands.limit(150).na.drop()
    assert(df2.numRows > 1)
    assembler.input(assembler.inputDf, df2)

    val dfPredict2 = dtc.output(dtc.outputDf).getResult(TEST_WAIT_TIME)
    assert(dfPredict2.numRows === df2.numRows)
    assert(dfPredict2.numCols === df2.numCols + 5)
    assert(dfPredict2.schema("band_type_predict").storageType === StorageTypes.DoubleType)
    assert(dtc.getModel() eq dtcModel)

    // use the resulting model
    val dfPredict2b = dtcModel.transform(stringIndexer.output(stringIndexer.outputDf).getResult(TEST_WAIT_TIME))
    assert(dfPredict2b.numRows === df2.numRows)
    assert(dfPredict2b.numCols === df2.numCols + 5)
    assert(dfPredict2b.schema("band_type_predict").storageType === StorageTypes.DoubleType)

    // change a stateful input, model will be retrained
    dtc.input(dtc.predictionCol, "band_type_predict_2")
    val dtcModel2 = dtc.getModel()
    val dfPredict3 = dtc.output(dtc.outputDf).getResult(TEST_WAIT_TIME)
    assert(dfPredict3.numRows === df2.numRows)
    assert(dfPredict3.numCols === df2.numCols + 5)
    assert(dfPredict3.schema("band_type_predict_2").storageType === StorageTypes.DoubleType)
    assert(dtcModel2 ne dtcModel)

    // use the resulting model
    val dfPredict4 = dtcModel2.transform(stringIndexer.output(stringIndexer.outputDf).getResult(TEST_WAIT_TIME))
    assert(dfPredict4.numRows === df2.numRows)
    assert(dfPredict4.numCols === df2.numCols + 5)
    assert(dfPredict4.schema("band_type_predict_2").storageType === StorageTypes.DoubleType)
  }
}
