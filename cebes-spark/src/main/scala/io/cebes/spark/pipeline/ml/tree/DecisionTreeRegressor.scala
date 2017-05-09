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
import org.apache.spark.ml.regression.{DecisionTreeRegressor => SparkDecisionTreeRegressor}

trait DecisionTreeRegressorInputs extends DecisionTreeInputs with HasProbabilityCol with HasRawPredictionCol {

  val varianceCol: InputSlot[String] = inputSlot[String]("varianceCol",
    "Column name for the biased sample variance of prediction", None, optional = true)

  val impurity: InputSlot[String] = inputSlot[String]("impurity",
    "Criterion used for information gain calculation (case-insensitive). Supported options: \"variance\"",
    Some("variance"), SlotValidators.oneOf("variance"))
}

/**
  * A thin wrapper around Spark's DecisionTreeRegressor
  * It supports both continuous and categorical features.
  */
class DecisionTreeRegressor @Inject()(dfFactory: SparkDataframeFactory)
  extends SparkEstimator with DecisionTreeRegressorInputs {

  override protected def computeStatefulOutput(inputs: SlotValueMap, stateSlot: OutputSlot[Any]): Any = {
    val sparkEstimator = new SparkDecisionTreeRegressor()
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
    inputs.get(varianceCol).foreach(sparkEstimator.setVarianceCol)

    val df = getSparkDataframe(inputs(inputDf)).sparkDf
    DecisionTreeRegressorModel(HasId.randomId, sparkEstimator.fit(df), dfFactory)
  }
}

case class DecisionTreeRegressorModel(id: UUID, sparkTransformer: Transformer,
                                      dfFactory: SparkDataframeFactory)
  extends SparkModel with DecisionTreeRegressorInputs
