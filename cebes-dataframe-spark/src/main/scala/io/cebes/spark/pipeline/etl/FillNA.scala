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
import io.cebes.pipeline.models._
import io.cebes.pipeline.stages.UnaryTransformer

/**
  * fill NA cells with given value, either double or string
  */
case class FillNA() extends UnaryTransformer {

  val stringValue: InputSlot[String] = inputSlot[String]("stringValue", "String value used to replace NAs", None)
  val doubleValue: InputSlot[String] = inputSlot[String]("doubleValue", "Double value used to replace NAs", None)
  val cols: InputSlot[Array[String]] = inputSlot[Array[String]]("cols", "Column names to consider", Some(Array()))

  override protected def transform(df: Dataframe, inputs: SlotValueMap): Dataframe = {
    (inputs.get(stringValue), inputs.get(doubleValue)) match {
      case (Some(s), None) =>
        df.na.fill(s, inputs(cols))
      case (None, Some(d)) =>
        df.na.fill(d, inputs(cols))
      case _ =>
        throw new IllegalArgumentException("Either stringValue or doubleValue must be provided")
    }
  }
}
