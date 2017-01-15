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
import io.cebes.pipeline.models.{BooleanParam, DoubleParam, LongParam, ParamValidators}
import io.cebes.pipeline.stages.UnaryTransformer

/**
  * Randomly sample n rows from a [[Dataframe]], returns another [[Dataframe]]
  */
class Sample extends UnaryTransformer {

  val withReplacement = BooleanParam("withReplacement", Some(true), "Whether to sample with replacement")
  val fraction = DoubleParam("fraction", Some(0.5), "Fraction of rows to generate", ParamValidators.greaterOrEqual(0))
  val seed = LongParam("seed", Some(42), "Seed for sampling")

  override protected def transform(df: Dataframe): Dataframe = {
    df.sample(param(withReplacement), param(fraction), param(seed))
  }
}
