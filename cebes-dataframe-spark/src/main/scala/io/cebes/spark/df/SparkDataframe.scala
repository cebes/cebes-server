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
import io.cebes.df.sample.DataSample
import io.cebes.df.schema.{ColumnTypes, Schema}
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
    * Sampling functions
    */

  /**
    * Get the first n rows. If the [[Dataframe]] has less than n rows, all rows will be returned.
    * Since the data will be gathered to the memory of a single JVM process,
    * calling this function with big n might cause [[OutOfMemoryError]]
    *
    * @param n number of rows to take
    * @return a [[DataSample]] object containing the data.
    */
  def take(n: Int = 1): DataSample = {
    val rows = sparkDf.take(n)
    val cols = schema.columns.zipWithIndex.map {
      case (c, idx) =>
        rows.map { r =>
          c.dataType match {
            case ColumnTypes.STRING =>
              if (r.isNullAt(idx)) null else r.getString(idx)
            case ColumnTypes.BOOLEAN =>
              if (r.isNullAt(idx)) null else r.getBoolean(idx)
            case ColumnTypes.BYTE =>
              if (r.isNullAt(idx)) null else r.getByte(idx)
            case ColumnTypes.SHORT =>
              if (r.isNullAt(idx)) null else r.getShort(idx)
            case ColumnTypes.INT =>
              if (r.isNullAt(idx)) null else r.getInt(idx)
            case ColumnTypes.LONG =>
              if (r.isNullAt(idx)) null else r.getLong(idx)
            case ColumnTypes.FLOAT =>
              if (r.isNullAt(idx)) null else r.getFloat(idx)
            case ColumnTypes.DOUBLE =>
              if (r.isNullAt(idx)) null else r.getDouble(idx)
            case ColumnTypes.VECTOR =>
              if (r.isNullAt(idx)) null else r.getSeq[Double](idx).toArray
            case ColumnTypes.BINARY =>
              if (r.isNullAt(idx)) null else r.getAs[Array[Byte]](idx)
            case ColumnTypes.DATE =>
              if (r.isNullAt(idx)) null else r.getDate(idx)
            case ColumnTypes.TIMESTAMP =>
              if (r.isNullAt(idx)) None else r.getTimestamp(idx)
            case t => throw new IllegalArgumentException(s"Unrecognized cebes type: ${t.toString}")
          }
        }.toSeq
    }
    new DataSample(schema.copy(), cols)
  }

  /**
    * Randomly sample n rows, return the result as a [[Dataframe]]
    *
    * @param withReplacement Sample with replacement or not.
    * @param fraction        Fraction of rows to generate.
    * @param seed            Seed for sampling.
    * @return a [[Dataframe]] object containing the data.
    */
  def sample(withReplacement: Boolean, fraction: Double, seed: Long): Dataframe = {
    new SparkDataframe(sparkDf.sample(withReplacement, fraction, seed))
  }

  /**
    * Create a temporary view of this Dataframe,
    * so you can run SQL commands against
    *
    * @param name name of the view
    */
  override def createTempView(name: String) = {
    sparkDf.createTempView(name)
  }

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
