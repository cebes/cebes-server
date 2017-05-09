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
import io.cebes.pipeline.models.{OutputSlot, SlotValueMap}
import io.cebes.spark.df.SparkDataframeFactory
import io.cebes.spark.pipeline.ml.traits.{SparkEstimator, SparkModel}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.{MaxAbsScaler => SparkMaxAbsScaler}

trait MaxAbsScalerInputs extends HasInputCol with HasOutputCol {

}

/**
  * Light wrapper of Spark's MaxAbsScaler
  * Rescale each feature individually to range [-1, 1] by dividing through
  * the largest maximum absolute value in each feature.
  * It does not shift/center the data, and thus does not destroy any sparsity.
  *
  * @see [[StandardScaler]], [[MinMaxScaler]]
  */
case class MaxAbsScaler @Inject()(dfFactory: SparkDataframeFactory)
  extends SparkEstimator with MaxAbsScalerInputs {


  override protected def computeStatefulOutput(inputs: SlotValueMap, stateSlot: OutputSlot[Any]): Any = {
    val scaler = new SparkMaxAbsScaler()
      .setInputCol(inputs(inputCol))
      .setOutputCol(inputs(outputCol))

    val df = getSparkDataframe(inputs(inputDf)).sparkDf
    MaxAbsScalerModel(HasId.randomId, scaler.fit(df), dfFactory)
  }
}

case class MaxAbsScalerModel(id: UUID, sparkTransformer: Transformer,
                             dfFactory: SparkDataframeFactory)
  extends SparkModel with MaxAbsScalerInputs
