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
 *
 * Created by phvu on 14/11/2016.
 */

package io.cebes.spark.df.expressions

import java.util.UUID

import io.cebes.df.expressions.LeafExpression
import org.apache.spark.sql.Column

/**
  * The most primitive expression on Spark: a Spark column object.
  * We also include the ID of the dataframe and the column name, for the sake of serialization.
  *
  * @param dfId    ID of the Dataframe that this column belong to
  * @param colName name of the (Spark's DataFrame) column
  */
case class SparkPrimitiveExpression(dfId: UUID, colName: String, sparkCol: Option[Column]) extends LeafExpression
