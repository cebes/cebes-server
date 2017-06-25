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

import java.util.UUID

import com.google.inject.Inject
import io.cebes.common.HasId
import io.cebes.pipeline.ml._
import io.cebes.pipeline.models._
import io.cebes.spark.df.SparkDataframeFactory
import io.cebes.spark.pipeline.ml.traits._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.classification.{DecisionTreeClassifier => SparkDecisionTreeClassifier}
import org.apache.spark.ml.util.MLWritable

trait DecisionTreeClassifierInputs extends DecisionTreeInputs with HasProbabilityCol with HasRawPredictionCol {

  val thresholds: InputSlot[Array[Double]] = inputSlot[Array[Double]]("thresholds",
    "Param for Thresholds in multi-class classification to adjust the probability of predicting each class. " +
      "Array must have length equal to the number of classes, with values > 0 excepting that at most " +
      "one value may be 0. The class with largest value p/t is predicted, where p is the original " +
      "probability of that class and t is the class's threshold.", None,
    RequiredSlotValidator((t: Array[Double]) => t.forall(_ >= 0) && t.count(_ == 0) <= 1, ""),
    optional = true)

  val impurity: InputSlot[String] = inputSlot[String]("impurity",
    "Criterion used for information gain calculation (case-insensitive). Supported options: \"entropy\", \"gini\"",
    Some("gini"), SlotValidators.oneOf("entropy", "gini"))
}

/**
  * thin wrapper around Spark's DecisionTreeClassifier
  * It supports both continuous and categorical features.
  */
class DecisionTreeClassifier @Inject()(dfFactory: SparkDataframeFactory)
  extends SparkEstimator with DecisionTreeClassifierInputs {

  override protected def estimate(inputs: SlotValueMap): SparkModel = {
    val sparkEstimator = new SparkDecisionTreeClassifier()
      .setFeaturesCol(inputs(featuresCol))
      .setLabelCol(inputs(labelCol))
      .setPredictionCol(inputs(predictionCol))
      .setCheckpointInterval(inputs(checkpointInterval))
      .setImpurity(inputs(impurity))
      .setMaxBins(inputs(maxBins))
      .setMaxDepth(inputs(maxDepth))
      .setMinInfoGain(inputs(minInfoGain))
      .setMinInstancesPerNode(inputs(minInstancesPerNode))
      .setSeed(inputs(seed))
      .setMaxMemoryInMB(inputs(maxMemoryInMB))
      .setCacheNodeIds(inputs(cacheNodeIds))

    inputs.get(probabilityCol).foreach(sparkEstimator.setProbabilityCol)
    inputs.get(rawPredictionCol).foreach(sparkEstimator.setRawPredictionCol)
    inputs.get(thresholds).foreach(sparkEstimator.setThresholds)

    val df = getSparkDataframe(inputs(inputDf)).sparkDf
    val sparkModel = sparkEstimator.fit(df)
    DecisionTreeClassifierModel(HasId.randomId, sparkModel, dfFactory)
  }
}

case class DecisionTreeClassifierModel(id: UUID,
                                       sparkTransformer: Transformer with MLWritable,
                                       dfFactory: SparkDataframeFactory)
  extends SparkModel with DecisionTreeClassifierInputs
