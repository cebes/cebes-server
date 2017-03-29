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
import io.cebes.pipeline.models.{InputSlot, SlotValueMap}
import io.cebes.pipeline.stages.UnaryTransformer

/**
  * Returns a new [[Dataframe]] that drops rows containing less than
  * `minNonNulls` non-null and non-NaN values in the specified columns.
  */
case class DropNA() extends UnaryTransformer {

  val minNonNulls: InputSlot[Int] = inputSlot[Int]("minNonNulls", "Minimum number of non-null values", Some(0))
  val cols: InputSlot[Array[String]] = inputSlot[Array[String]]("cols", "Array of column names", Some(Array()))

  override protected def transform(df: Dataframe, inputs: SlotValueMap, states: SlotValueMap): Dataframe = {
    df.na.drop(inputs(minNonNulls), inputs(cols))
  }
}
