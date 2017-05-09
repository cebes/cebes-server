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

import com.google.inject.Inject
import io.cebes.df.Dataframe
import io.cebes.pipeline.models.{InputSlot, SlotValueMap}
import io.cebes.spark.df.SparkDataframeFactory
import org.apache.spark.ml.feature.{Normalizer => SparkNormalizer}

/**
  * Light wrapper of Spark's Normalizer
  * Normalize a vector to have unit norm using the given p-norm.
  *
  */
case class Normalizer @Inject()(dfFactory: SparkDataframeFactory)
  extends SparkUnaryTransformer with HasInputCol with HasOutputCol {

  val p: InputSlot[Double] = inputSlot[Double]("p",
    "Normalization in Lp space, default p=2", Some(2))

  /** Implement this function to do the transformation */
  override protected def transform(df: Dataframe, inputs: SlotValueMap, states: SlotValueMap): Dataframe = {
    val normalizer = new SparkNormalizer()
      .setInputCol(inputs(inputCol))
      .setOutputCol(inputs(outputCol))
      .setP(inputs(p))

    sparkTransform(normalizer, df, dfFactory, inputs(outputCol))
  }
}
