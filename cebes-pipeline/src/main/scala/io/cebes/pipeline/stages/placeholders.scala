/* Copyright 2017 The Cebes Authors. All Rights Reserved.
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
package io.cebes.pipeline.stages

import io.cebes.df.{Column, Dataframe}
import io.cebes.pipeline.models.{InputSlot, OutputSlot, SlotValueMap, Stage}

import scala.reflect.ClassTag

/**
  * Hold a value and throw exception if it is not filled
  */
class Placeholder[T: ClassTag] extends Stage {

  val inputVal: InputSlot[T] = inputSlot[T]("inputVal", "", None, stateful = true)
  val outputVal: OutputSlot[T] = outputSlot[T]("outputVal", "", None, stateful = false)

  override protected def computeStatelessOutputs(inputs: SlotValueMap, states: SlotValueMap): SlotValueMap = {
    SlotValueMap(outputVal, inputs(inputVal))
  }
}

class ValuePlaceholder extends Placeholder[Any]

class DataframePlaceholder extends Placeholder[Dataframe]

class ColumnPlaceholder extends Placeholder[Column]
