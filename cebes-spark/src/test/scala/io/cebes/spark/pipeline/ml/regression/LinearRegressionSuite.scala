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

import java.nio.file.Files

import io.cebes.df.types.StorageTypes
import io.cebes.pipeline.exporter.PipelineExporter
import io.cebes.pipeline.factory.ModelFactory
import io.cebes.pipeline.json._
import io.cebes.pipeline.models.SlotDescriptor
import io.cebes.spark.helpers.{ImplicitExecutor, TestDataHelper, TestPipelineHelper}
import io.cebes.spark.json.CebesSparkJsonProtocol._
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

    val assembler = getInstance[VectorAssembler]
    assembler.input(assembler.inputCols, Array("viscosity", "proof_cut"))
      .input(assembler.outputCol, "features")
      .input(assembler.inputDf, df)

    val lr = getInstance[LinearRegression]
    lr.input(lr.featuresCol, "features")
      .input(lr.labelCol, "band_type")
      .input(lr.inputDf, assembler.output(assembler.outputDf))

    val ex0 = intercept[IllegalArgumentException] {
      lr.getModel()
    }
    assert(ex0.getMessage.contains("Column band_type must be of type NumericType but was actually of type StringType."))

    lr.input(lr.labelCol, "caliper")
      .input(lr.predictionCol, "caliper_predict")

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

    // use the resulting model
    val dfPredict2b = lrModel.transform(assembler.output(assembler.outputDf).getResult(TEST_WAIT_TIME))
    assert(dfPredict2b.numRows === df2.numRows)
    assert(dfPredict2b.numCols === df2.numCols + 2)
    assert(dfPredict2b.schema("caliper_predict").storageType === StorageTypes.DoubleType)

    // change a stateful input, model will be retrained
    lr.input(lr.predictionCol, "caliper_predict_2")
    val lrModel2 = lr.getModel()
    val dfPredict3 = lr.output(lr.outputDf).getResult(TEST_WAIT_TIME)
    assert(dfPredict3.numRows === df2.numRows)
    assert(dfPredict3.numCols === df2.numCols + 2)
    assert(dfPredict3.schema("caliper_predict_2").storageType === StorageTypes.DoubleType)
    assert(lrModel2 ne lrModel)

    // use the resulting model
    val dfPredict4 = lrModel2.transform(assembler.output(assembler.outputDf).getResult(TEST_WAIT_TIME))
    assert(dfPredict4.numRows === df2.numRows)
    assert(dfPredict4.numCols === df2.numCols + 2)
    assert(dfPredict4.schema("caliper_predict_2").storageType === StorageTypes.DoubleType)
  }

  test("LinearRegression serialization") {
    val df = getCylinderBands.limit(200).na.drop()
    assert(df.numRows > 1)

    val assembler = getInstance[VectorAssembler]
    assembler.input(assembler.inputCols, Array("viscosity", "proof_cut"))
      .input(assembler.outputCol, "features")
      .input(assembler.inputDf, df)

    val lr = getInstance[LinearRegression]
    lr.input(lr.featuresCol, "features")
      .input(lr.labelCol, "caliper")
      .input(lr.predictionCol, "caliper_predict")
      .input(lr.inputDf, assembler.output(assembler.outputDf))

    val lrModel = lr.getModel()
    assert(lrModel.isInstanceOf[LinearRegressionModel])
    val v = lrModel.input(lrModel.getInput("labelCol")).get
    assert(v.isInstanceOf[String])
    assert(v.asInstanceOf[String] === "caliper")

    val modelFactory = getInstance[ModelFactory]
    val lrModelDef = modelFactory.save(lrModel)
    val lrModel2 = modelFactory.create(lrModelDef)

    assert(lrModel2.isInstanceOf[LinearRegressionModel])
    assert(lrModel2.id === lrModel.id)
    val v2 = lrModel2.input(lrModel2.getInput("labelCol")).get
    assert(v2.isInstanceOf[String])
    assert(v2.asInstanceOf[String] === "caliper")
  }

  test("Import and export") {
    val df = getCylinderBands.limit(200).na.drop()
    assert(df.numRows > 1)

    val pplDef = PipelineDef(None, Array(
      StageDef("s1", "VectorAssembler", Map(
        "inputCols" -> ValueDef(Array("viscosity", "proof_cut")),
        "outputCol" -> ValueDef("features"))),
      StageDef("s2", "LinearRegression", Map(
        "featuresCol" -> ValueDef("features"),
        "labelCol" -> ValueDef("caliper"),
        "predictionCol" -> ValueDef("caliper_predict"),
        "inputDf" -> StageOutputDef("s1", "outputDf")))))

    val ppl = pipelineFactory.create(pplDef)
    assert(ppl.stages.size === 2)
    assert(ppl.stages.contains("s1") && ppl.stages.contains("s2"))
    assert(ppl.stages("s1").isInstanceOf[VectorAssembler])
    assert(ppl.stages("s2").isInstanceOf[LinearRegression])

    val exporter = getInstance[PipelineExporter]

    val testDir = Files.createTempDirectory("test-ppl-")

    // without models
    val exportedDir = result(exporter.export(ppl, testDir.toString))

    // re-import
    val ppl2 = exporter.imports(exportedDir)
    assert(ppl2.id === ppl.id)
    assert(ppl2.stages.size === 2)
    assert(ppl2.stages.contains("s1") && ppl2.stages.contains("s2"))
    assert(ppl2.stages("s1").isInstanceOf[VectorAssembler])
    assert(ppl2.stages("s2").isInstanceOf[LinearRegression])

    // train the model
    result(ppl.run(Seq(SlotDescriptor("s2", "model")), Map(SlotDescriptor("s1", "inputDf") -> df)))

    // export again, with models
    val exportedDir2 = result(exporter.export(ppl, testDir.toString))
    assert(exportedDir2 !== exportedDir)
    val ppl3 = exporter.imports(exportedDir2)
    assert(ppl3.id === ppl.id)
    assert(ppl3.stages.size === 2)
    assert(ppl3.stages.contains("s1") && ppl3.stages.contains("s2"))
    assert(ppl3.stages("s1").isInstanceOf[VectorAssembler])
    assert(ppl3.stages("s2").isInstanceOf[LinearRegression])
    val lrOutputs = result(ppl3.stages("s2").getOutputs())
    assert(lrOutputs.contains("model"))
    assert(lrOutputs("model").isInstanceOf[LinearRegressionModel])

    deleteRecursively(testDir.toFile)
  }
}
