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
package io.cebes.spark.pipeline.features

import java.util.UUID

import com.google.inject.Inject
import io.cebes.common.HasId
import io.cebes.pipeline.models.{InputSlot, SlotValueMap}
import io.cebes.spark.df.SparkDataframeFactory
import io.cebes.spark.pipeline.ml.traits.{SparkEstimator, SparkModel}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.{StandardScaler => SparkStandardScaler}
import org.apache.spark.ml.util.MLWritable

trait StandardScalerInputs extends HasInputCol with HasOutputCol {

  val withMean: InputSlot[Boolean] = inputSlot[Boolean]("withMean",
    "Whether to center the data with mean before scaling", Some(false))

  val withStd: InputSlot[Boolean] = inputSlot[Boolean]("withStd",
    "Whether to scale the data to unit standard deviation", Some(true))
}

/**
  * Light wrapper of Spark's StandardScaler
  * Standardizes features by removing the mean and scaling to unit variance using column
  * summary statistics on the samples in the training set.
  *
  * The "unit std" is computed using the corrected sample standard deviation,
  * which is computed as the square root of the unbiased sample variance.
  *
  * @see
  */
case class StandardScaler @Inject()(dfFactory: SparkDataframeFactory)
  extends SparkEstimator with StandardScalerInputs {

  override protected def estimate(inputs: SlotValueMap): SparkModel = {
    val scaler = new SparkStandardScaler()
      .setInputCol(inputs(inputCol))
      .setOutputCol(inputs(outputCol))
      .setWithMean(inputs(withMean))
      .setWithStd(inputs(withStd))

    val df = getSparkDataframe(inputs(inputDf)).sparkDf
    StandardScalerModel(HasId.randomId, scaler.fit(df), dfFactory)
  }
}

case class StandardScalerModel(id: UUID, sparkTransformer: Transformer with MLWritable,
                               dfFactory: SparkDataframeFactory)
  extends SparkModel with StandardScalerInputs
