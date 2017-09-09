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
package io.cebes.pipeline.ml

import java.util.concurrent.TimeUnit

import io.cebes.df.Dataframe
import io.cebes.pipeline.models.{InputSlot, OutputSlot, SlotValueMap, Stage}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

trait Evaluator extends Stage {

  val inputDf: InputSlot[Dataframe] = inputSlot[Dataframe]("inputDf", "The input dataframe", None, stateful = false)
  val metricValue: OutputSlot[Double] = outputSlot[Double]("metricValue",
    "The computed metric value", None, stateful = false)

  override def computeStatelessOutputs(inputs: SlotValueMap, states: SlotValueMap): SlotValueMap = {
    SlotValueMap(metricValue, evaluate(inputs(inputDf), inputs, states))
  }

  /**
    * Compute the metric and return its value
    *
    * @param atMost the maximum time allowed to wait. If it takes longer, the function will fail.
    * @return the metric value
    */
  def getMetricValue(atMost: Duration = Duration(2, TimeUnit.MINUTES))(implicit ec: ExecutionContext): Double = {
    output(metricValue).getResult(atMost)(ec)
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // to be overridden
  /////////////////////////////////////////////////////////////////////////////////////////////////

  /**
    * Override this to implement the actual metric computation
    */
  def evaluate(df: Dataframe, inputs: SlotValueMap, states: SlotValueMap): Double

  /**
    * Whether this metric is better when its value is larger
    */
  def isLargerBetter: Boolean = true
}
