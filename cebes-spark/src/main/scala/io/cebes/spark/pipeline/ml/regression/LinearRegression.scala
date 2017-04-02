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

import java.util.UUID

import com.google.inject.Inject
import io.cebes.common.HasId
import io.cebes.pipeline.models.{InputSlot, OutputSlot, SlotValidators, SlotValueMap}
import io.cebes.spark.df.SparkDataframeFactory
import io.cebes.spark.pipeline.ml.traits._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.regression.{LinearRegression => SparkLR}

trait LinearRegressionInputs extends HasFeaturesCol with HasLabelCol with HasPredictionCol {

  val aggregationDepth: InputSlot[Int] = inputSlot[Int]("aggregationDepth",
    "suggested depth for treeAggregate (>= 2)",
    Some(2), SlotValidators.greaterOrEqual(2))

  val elasticNetParam: InputSlot[Double] = inputSlot[Double]("elasticNetParam",
    "ElasticNet param, must be between [0, 1]. Value of 0 corresponds to L2 penalty, 1 corresponds to L1 penalty",
    Some(0.0), SlotValidators.between(0, 1))

  val fitIntercept: InputSlot[Boolean] = inputSlot[Boolean]("fitIntercept",
    "Whether to fit the intercept",
    Some(true))

  val maxIter: InputSlot[Int] = inputSlot[Int]("maxIter",
    "maximum number of iterations (>= 0)",
    Some(10), SlotValidators.greaterOrEqual(0))

  val regParam: InputSlot[Double] = inputSlot[Double]("regParam",
    "regularization parameter (>= 0)",
    Some(0.0), SlotValidators.greaterOrEqual(0.0))

  val standardization: InputSlot[Boolean] = inputSlot[Boolean]("standardization",
    "whether to standardize the training features before fitting the model",
    Some(true))

  val tolerance: InputSlot[Double] = inputSlot[Double]("tolerance",
    "the convergence tolerance for iterative algorithms (>= 0)",
    Some(1E-6), SlotValidators.greaterOrEqual(0.0))

  val weightCol: InputSlot[String] = inputSlot[String]("weightCol",
    "weight column name. If this is not set or empty, we treat all instance weights as 1.0",
    None, optional = true)

  val solver: InputSlot[String] = inputSlot[String]("solver",
    "Set the solver algorithm used for optimization. Valid values are 'l-bfgs', 'normal' or 'auto'",
    Some("auto"), SlotValidators.oneOf("auto", "l-bfgs", "normal"))
}

/**
  * Linear Regression model, a thin wrapper around Spark's Linear Regression
  */
class LinearRegression @Inject()(dfFactory: SparkDataframeFactory)
  extends SparkEstimator with LinearRegressionInputs {

  override protected def computeStatefulOutput(inputs: SlotValueMap, stateSlot: OutputSlot[Any]): Any = {
    val sparkEstimator = new SparkLR()
      .setFeaturesCol(inputs(featuresCol))
      .setLabelCol(inputs(labelCol))
      .setPredictionCol(inputs(predictionCol))
      .setAggregationDepth(inputs(aggregationDepth))
      .setElasticNetParam(inputs(elasticNetParam))
      .setFitIntercept(inputs(fitIntercept))
      .setMaxIter(inputs(maxIter))
      .setRegParam(inputs(regParam))
      .setStandardization(inputs(standardization))
      .setTol(inputs(tolerance))
      .setSolver(inputs(solver))

    inputs.get(weightCol).foreach(sparkEstimator.setWeightCol)

    val df = getSparkDataframe(inputs(inputDf)).sparkDf
    LinearRegressionModel(HasId.randomId, sparkEstimator.fit(df), dfFactory)
  }

  override protected def computeStatelessOutputs(inputs: SlotValueMap, states: SlotValueMap): SlotValueMap = {
    SlotValueMap(outputDf, states(model).transform(inputs(inputDf)))
  }
}

case class LinearRegressionModel(id: UUID, sparkTransformer: Transformer,
                                 dfFactory: SparkDataframeFactory)
  extends SparkModel with LinearRegressionInputs
