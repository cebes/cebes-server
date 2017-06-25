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
package io.cebes.spark.pipeline.ml.traits

import io.cebes.pipeline.ml.Estimator
import io.cebes.pipeline.models.{OutputSlot, SlotValueMap}
import io.cebes.spark.util.CebesSparkUtil

/**
  * Generic [[Estimator]] trait with helpers specialized for Spark ML models
  */
trait SparkEstimator extends Estimator with CebesSparkUtil {

  /**
    * Subclasses only needs to implement this function and returns a [[SparkModel]]
    */
  protected def estimate(inputs: SlotValueMap): SparkModel

  override protected def computeStatelessOutputs(inputs: SlotValueMap, states: SlotValueMap): SlotValueMap = {
    SlotValueMap(outputDf, states(model).transform(inputs(inputDf)))
  }

  override protected def computeStatefulOutput(inputs: SlotValueMap, stateSlot: OutputSlot[Any]): Any = {
    require(stateSlot.equals(this.model), s"${getClass.getName} only has one stateful output slot, " +
      s"but required to compute for ${stateSlot.toString}")

    // call estimate and copy all the parameters to the estimated model
    estimate(inputs).copyInputs(inputs)
  }
}
