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
package io.cebes.spark.pipeline.ml.evaluation

import io.cebes.spark.helpers.{ImplicitExecutor, TestDataHelper, TestPipelineHelper}
import io.cebes.spark.pipeline.features.VectorAssembler
import io.cebes.spark.pipeline.ml.regression.LinearRegression
import org.scalatest.FunSuite

class RegressionEvaluatorSuite extends FunSuite with ImplicitExecutor with TestDataHelper with TestPipelineHelper {

  override def beforeAll(): Unit = {
    super.beforeAll()
    createOrReplaceCylinderBands()
  }

  test("simple case") {
    val df = getCylinderBands.select("wax", "hardener", "customer").na.drop("any")
    assert(df.numRows > 0)

    val evaluator = new RegressionEvaluator()
    evaluator.input(evaluator.labelCol, "hardener")
      .input(evaluator.predictionCol, "wax")
      .input(evaluator.inputDf, df)

    Array("mse", "rmse", "r2", "mae").foreach { m =>
      evaluator.input(evaluator.metricName, m)
      val d = evaluator.getMetricValue(TEST_WAIT_TIME)
      assert(!d.isNaN)
    }

    val ex1 = intercept[IllegalArgumentException] {
      evaluator.input(evaluator.metricName, "invalid")
    }
    assert(ex1.getMessage.endsWith("slot metricName: invalid value 'invalid'. Allowed values are: mse, rmse, r2, mae"))

    evaluator.input(evaluator.labelCol, "customer")
    val ex2 = intercept[IllegalArgumentException] {
      evaluator.getMetricValue(TEST_WAIT_TIME)
    }
    assert(ex2.getMessage.endsWith("Column customer must be of type NumericType but was actually of type StringType."))
  }

  test("with linear regression") {
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

    val evaluator = new RegressionEvaluator()
    evaluator.input(evaluator.labelCol, "caliper")
      .input(evaluator.predictionCol, "caliper_predict")
      .input(evaluator.metricName, "rmse")
      .input(evaluator.inputDf, lr.output(lr.outputDf))
    val d = evaluator.getMetricValue(TEST_WAIT_TIME)
    assert(d > 0)
    assert(evaluator.output(evaluator.metricValue).getResult(TEST_WAIT_TIME) === d)
  }
}
