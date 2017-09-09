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
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator => SparkMCE}

/**
  * Compute metrics for multi-class classification problems, expects two inputs: the label column
  * and the prediction column
  */
class MulticlassClassificationEvaluator extends Evaluator
  with HasLabelCol with HasPredictionCol with CebesSparkUtil {

  val metricName: InputSlot[String] = inputSlot[String]("metricName",
    "The metric to be computed: f1, weightedPrecision, weightedRecall, or accuracy (default)",
    Some("accuracy"), SlotValidators.oneOf("f1", "weightedPrecision", "weightedRecall", "accuracy"))

  override def evaluate(df: Dataframe, inputs: SlotValueMap, states: SlotValueMap): Double = {
    val evaluator = new SparkMCE()
      .setLabelCol(inputs(labelCol))
      .setPredictionCol(inputs(predictionCol))
      .setMetricName(inputs(metricName))
    evaluator.evaluate(getSparkDataframe(df).sparkDf)
  }

  /**
    * All supported metrics of [[MulticlassClassificationEvaluator]] are better when they are larger
    */
  override def isLargerBetter: Boolean = true
}
