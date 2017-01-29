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

import io.cebes.df.Dataframe
import io.cebes.pipeline.models._

/**
  * Transforms a [[io.cebes.df.Dataframe]] into another [[io.cebes.df.Dataframe]]
  */
trait UnaryTransformer extends Stage {

  val inputDf: InputSlot[Dataframe] = inputSlot[Dataframe]("inputDf", "The input dataframe", None)
  val outputDf: OutputSlot[Dataframe] = outputSlot[Dataframe]("outputDf", "The output dataframe", None)

  override protected def run(inputs: SlotValueMap): SlotValueMap = {
    SlotValueMap(Seq(outputDf -> transform(inputs(inputDf), inputs)))
  }

  /** Implement this function to do the transformation */
  protected def transform(df: Dataframe, inputs: SlotValueMap): Dataframe
}
