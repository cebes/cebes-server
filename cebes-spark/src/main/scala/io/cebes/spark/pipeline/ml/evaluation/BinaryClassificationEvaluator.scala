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
import io.cebes.pipeline.ml.{Evaluator, HasLabelCol, HasRawPredictionCol}
import io.cebes.pipeline.models.{InputSlot, SlotValidators, SlotValueMap}
import io.cebes.spark.util.CebesSparkUtil
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator => SparkBCE}

/**
  * Compute several metrics for binary classification models, where the raw prediction column
  * contains either predicted label (0 or 1) or probabilities of class 1
  */
class BinaryClassificationEvaluator extends Evaluator with HasLabelCol with HasRawPredictionCol with CebesSparkUtil {

  val metricName: InputSlot[String] = inputSlot[String]("metricName",
    "The metric to be computed: areaUnderROC (default) or areaUnderPR",
    Some("areaUnderROC"), SlotValidators.oneOf("areaUnderROC", "areaUnderPR"))

  /**
    * Override this to implement the actual metric computation
    */
  override def evaluate(df: Dataframe, inputs: SlotValueMap, states: SlotValueMap): Double = {
    val sparkBCE = new SparkBCE().setLabelCol(inputs(labelCol))
      .setRawPredictionCol(inputs(rawPredictionCol))
      .setMetricName(inputs(metricName))

    sparkBCE.evaluate(getSparkDataframe(df).sparkDf)
  }

  /**
    * All supported metrics of [[BinaryClassificationEvaluator]] are better when they are larger
    */
  override def isLargerBetter: Boolean = true
}
