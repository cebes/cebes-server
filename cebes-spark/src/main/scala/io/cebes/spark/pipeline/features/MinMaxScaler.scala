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
import org.apache.spark.ml.feature.{MinMaxScaler => SparkMinMaxScaler}
import org.apache.spark.ml.util.MLWritable

trait MinMaxScalerInputs extends HasInputCol with HasOutputCol {

  val min: InputSlot[Double] = inputSlot[Double]("min",
    "Lower bound after transformation, shared by all features", Some(0.0))

  val max: InputSlot[Double] = inputSlot[Double]("max",
    "Upper bound after transformation, shared by all features", Some(1.0))
}

/**
  * Light wrapper of Spark's MinMaxScaler
  *
  * Rescale each feature individually to a common range [min, max] linearly using column summary
  * statistics, which is also known as min-max normalization or Rescaling. The rescaled value for
  * feature E is calculated as:
  *
  * <blockquote>
  * $$
  * Rescaled(e_i) = \frac{e_i - E_{min}}{E_{max} - E_{min}} * (max - min) + min
  * $$
  * </blockquote>
  *
  * For the case $E_{max} == E_{min}$, $Rescaled(e_i) = 0.5 * (max + min)$.
  *
  * @note Since zero values will probably be transformed to non-zero values, output of the
  *       transformer will be DenseVector even for sparse input.
  * @see [[StandardScaler]]
  */
case class MinMaxScaler @Inject()(dfFactory: SparkDataframeFactory)
  extends SparkEstimator with MinMaxScalerInputs {

  override protected def estimate(inputs: SlotValueMap): SparkModel = {
    val scaler = new SparkMinMaxScaler()
      .setInputCol(inputs(inputCol))
      .setOutputCol(inputs(outputCol))
      .setMin(inputs(min))
      .setMax(inputs(max))

    val df = getSparkDataframe(inputs(inputDf)).sparkDf
    MinMaxScalerModel(HasId.randomId, scaler.fit(df), dfFactory)
  }
}

case class MinMaxScalerModel(id: UUID, sparkTransformer: Transformer with MLWritable,
                             dfFactory: SparkDataframeFactory)
  extends SparkModel with MinMaxScalerInputs
