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
package io.cebes.spark.pipeline.etl

import io.cebes.df.Dataframe
import io.cebes.pipeline.models.{InputSlot, SlotValidators, SlotValueMap}
import io.cebes.pipeline.stages.UnaryTransformer

/**
  * Randomly sample n rows from a [[Dataframe]], returns another [[Dataframe]]
  */
case class Sample() extends UnaryTransformer {

  val withReplacement: InputSlot[Boolean] = inputSlot[Boolean]("withReplacement",
    "Whether to sample with replacement", Some(true))
  val fraction: InputSlot[Double] = inputSlot[Double]("fraction",
    "Fraction of rows to generate", Some(0.5), SlotValidators.greaterOrEqual(0))
  val seed: InputSlot[Long] = inputSlot[Long]("seed", "Seed for sampling", Some(42))

  override protected def transform(df: Dataframe, inputs: SlotValueMap, states: SlotValueMap): Dataframe = {
    df.sample(inputs(withReplacement), inputs(fraction), inputs(seed))
  }
}
