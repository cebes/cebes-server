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

import io.cebes.df.{Column, Dataframe}
import io.cebes.pipeline.models.{InputSlot, SlotValidators, SlotValueMap}
import io.cebes.pipeline.stages.BinaryTransformer

/** Do a Join between 2 dataframes */
case class Join() extends BinaryTransformer {

  val joinType: InputSlot[String] = inputSlot[String]("joinType",
    "Type of the join, must be one of: `inner`, `outer`, `left_outer`, `right_outer`, `leftsemi`",
    Some("inner"),
    SlotValidators.oneOf("inner", "outer", "left_outer", "right_outer", "leftsemi"))

  val joinExprs: InputSlot[Column] = inputSlot[Column]("joinExprs", "The join expression", None)

  override protected def transform(left: Dataframe, right: Dataframe, inputs: SlotValueMap): Dataframe = {
    left.join(right, inputs(joinExprs), inputs(joinType))
  }
}
