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
import org.apache.spark.ml.feature.{OneHotEncoder => SparkOneHotEncoder}

/**
  * Light wrapper around Spark's OneHotEncoder, which transforms a column of category indices
  * to a column of binary vectors, with at most a single one-value per row indicating the input category index.
  *
  * When `OneHotEncoder!.dropLast` is enable (default), an input column of N categories
  * will create the output column with vectors of size N-1.
  * This is to avoid the situation when the vector entries sum up to one, making them linearly dependent.
  */
case class OneHotEncoder @Inject()(dfFactory: SparkDataframeFactory)
  extends SparkUnaryTransformer with HasInputCol with HasOutputCol {

  val dropLast: InputSlot[Boolean] = inputSlot[Boolean]("dropLast",
    "Whether to drop the last category in the encoded vector (default: true)", Some(true))

  override protected def transform(df: Dataframe, inputs: SlotValueMap, states: SlotValueMap): Dataframe = {
    val encoder = new SparkOneHotEncoder()
      .setInputCol(inputs(inputCol))
      .setOutputCol(inputs(outputCol))
      .setDropLast(inputs(dropLast))

    sparkTransform(encoder, df, dfFactory, inputs(outputCol))
  }
}
