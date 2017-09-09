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

import io.cebes.df.functions
import io.cebes.df.types.StorageTypes
import io.cebes.spark.helpers.{ImplicitExecutor, TestDataHelper, TestPipelineHelper}
import org.scalatest.FunSuite

class MulticlassClassificationEvaluatorSuite extends FunSuite with ImplicitExecutor
  with TestDataHelper with TestPipelineHelper {

  override def beforeAll(): Unit = {
    super.beforeAll()
    createOrReplaceCylinderBands()
  }

  test("simple case") {
    val df = getCylinderBands.select(
      functions.floor(functions.col("proof_cut") / 10).alias("proof_cut_level"),
      functions.floor(functions.col("viscosity") / 10).alias("viscosity_level").cast(StorageTypes.DoubleType),
      functions.col("customer")).na.drop()
    assert(df.numRows > 0)

    val evaluator = new MulticlassClassificationEvaluator()
    evaluator.input(evaluator.labelCol, "proof_cut_level")
      .input(evaluator.predictionCol, "viscosity_level")
      .input(evaluator.inputDf, df)

    Array("f1", "weightedPrecision", "weightedRecall", "accuracy").foreach { m =>
      evaluator.input(evaluator.metricName, m)
      val d = evaluator.getMetricValue(TEST_WAIT_TIME)
      assert(d >= 0)
      assert(evaluator.isLargerBetter)
    }

    val ex1 = intercept[IllegalArgumentException] {
      evaluator.input(evaluator.metricName, "invalid")
    }
    assert(ex1.getMessage.endsWith("slot metricName: invalid value 'invalid'. " +
      "Allowed values are: f1, weightedPrecision, weightedRecall, accuracy"))

    evaluator.input(evaluator.labelCol, "customer")
    val ex2 = intercept[IllegalArgumentException] {
      evaluator.getMetricValue(TEST_WAIT_TIME)
    }
    assert(ex2.getMessage.endsWith("Column customer must be of type NumericType but was actually of type StringType."))
  }
}
