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
 * Created by phvu on 31/08/16.
 */

package io.cebes.spark.df

import java.util.UUID

import io.cebes.df.Dataframe
import io.cebes.df.schema.Schema
import io.cebes.spark.df.schema.SparkSchemaUtils
import org.apache.spark.sql.DataFrame

/**
  * Dataframe wrapper on top of Spark's DataFrame
  *
  * @param sparkDf the spark's DataFrame object
  */
class SparkDataframe(val sparkDf: DataFrame) extends Dataframe {

  override lazy val schema: Schema = SparkSchemaUtils.getSchema(sparkDf)

  override val id: UUID = UUID.randomUUID()

  /**
    * Number of rows
    *
    * @return a long
    */
  override def numRows: Long = sparkDf.count()

  /**
    * Apply a new schema to this data frame
    *
    * @param newSchema the new Schema
    * @return a new dataframe with the new Schema
    */
  override def applySchema(newSchema: Schema): Dataframe = {
    if (schema.numCols != newSchema.numCols) {
      throw new IllegalArgumentException(s"Incompatible schema: current schema has ${schema.numCols} columns," +
        s" but the new schema has ${newSchema.numCols} columns")
    }
    val sparkCols = schema.columns.zip(newSchema.columns).map { case (currentCol, newCol) =>
      sparkDf(currentCol.name).as(newCol.name).cast(SparkSchemaUtils.cebesTypesToSpark(newCol.dataType))
    }
    new SparkDataframe(sparkDf.select(sparkCols: _*))
  }
}
