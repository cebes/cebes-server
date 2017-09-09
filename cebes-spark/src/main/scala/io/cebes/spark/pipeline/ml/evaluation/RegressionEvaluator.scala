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

import io.cebes.df.Dataframe
import io.cebes.pipeline.ml.{Evaluator, HasLabelCol, HasPredictionCol}
import io.cebes.pipeline.models.{InputSlot, SlotValidators, SlotValueMap}
import io.cebes.spark.util.CebesSparkUtil
import org.apache.spark.ml.evaluation.{RegressionEvaluator => SparkRE}

/**
  * Compute metrics for regression problems, expecting 2 inputs: label column and prediction column.
  *
  * Supported metrics:
  *  - `"rmse"` (default): root mean squared error
  *  - `"mse"`: mean squared error
  *  - `"r2"`: R^2^ metric
  *  - `"mae"`: mean absolute error
  */
class RegressionEvaluator extends Evaluator
  with HasLabelCol with HasPredictionCol with CebesSparkUtil {

  val metricName: InputSlot[String] = inputSlot[String]("metricName",
    "The metric to be computed: mse, rmse (default), r2, or mae",
    Some("rmse"), SlotValidators.oneOf("mse", "rmse", "r2", "mae"))

  override def evaluate(df: Dataframe, inputs: SlotValueMap, states: SlotValueMap): Double = {
    val evaluator = new SparkRE()
      .setLabelCol(inputs(labelCol))
      .setPredictionCol(inputs(predictionCol))
      .setMetricName(inputs(metricName))
    evaluator.evaluate(getSparkDataframe(df).sparkDf)
  }

  /**
    * Whether the metric being used is better when it is larger.
    * This does not support the case when [[metricName]] is assigned to be the output of another stage
    */
  override def isLargerBetter: Boolean = input(metricName).get match {
    case "rmse" => false
    case "mse" => false
    case "r2" => true
    case "mae" => false
  }
}
