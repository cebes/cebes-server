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
import io.cebes.pipeline.stages.BinaryTransformer

/** Do a Join between 2 dataframes */
case class Join() extends BinaryTransformer {

  val joinType = StringParam("joinType", Some("inner"),
    "Type of the join, must be one of: `inner`, `outer`, `left_outer`, `right_outer`, `leftsemi`",
    ParamValidators.oneOf("inner", "outer", "left_outer", "right_outer", "leftsemi"))

  val joinExprs = ColumnParam("joinExprs", None, "The join expression")

  override protected def transform(left: Dataframe, right: Dataframe): Dataframe = {
    left.join(right, param(joinExprs), param(joinType))
  }
}
