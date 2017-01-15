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

  override protected final val _inputs: Seq[Slot[PipelineMessage]] = Seq(
    DataframeSlot("input", "The input dataframe")
  )

  override protected final val _outputs: Seq[Slot[PipelineMessage]] = Seq(
    DataframeSlot("output", "The output dataframe")
  )

  override protected def run(inputs: Seq[PipelineMessage]): Seq[PipelineMessage] = {
    Seq(DataframeMessage(transform(inputs.head.df)))
  }

  /** Implement this function to do the transformation */
  protected def transform(df: Dataframe): Dataframe
}
