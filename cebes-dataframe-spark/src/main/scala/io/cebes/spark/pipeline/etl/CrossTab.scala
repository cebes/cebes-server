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
import io.cebes.pipeline.models.StringParam
import io.cebes.pipeline.stages.UnaryTransformer

/**
  * Computes a pair-wise frequency table of the given columns. Also known as a contingency table.
  */
case class CrossTab() extends UnaryTransformer {

  val col1 = StringParam("col1", None, "Name of the first column")
  val col2 = StringParam("col2", None, "Name of the second column")

  override protected def transform(df: Dataframe): Dataframe = {
    df.stat.crosstab(param(col1), param(col2))
  }
}
