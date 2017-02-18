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
package io.cebes.pipeline.stages

import io.cebes.pipeline.models._
import io.cebes.df.Dataframe

/**
  * Transform 2 [[Dataframe]] into another [[Dataframe]]
  */
trait BinaryTransformer extends Stage {

  val leftDf: InputSlot[Dataframe] = inputSlot[Dataframe]("leftDf", "Left input to the transformation", None)
  val rightDf: InputSlot[Dataframe] = inputSlot[Dataframe]("rightDf", "Right input to the transformation", None)
  val outputDf: OutputSlot[Dataframe] = outputSlot[Dataframe]("outputDf", "Result of transformation", None)

  override protected def run(inputs: SlotValueMap): SlotValueMap = {
    SlotValueMap(outputDf, transform(inputs(leftDf), inputs(rightDf), inputs))
  }

  /** Implement this function to do the transformation */
  protected def transform(left: Dataframe, right: Dataframe, inputs: SlotValueMap): Dataframe
}
