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
import io.cebes.df.schema.VariableTypes.VariableType
import io.cebes.df.schema.{Column, Schema, StorageTypes, VariableTypes}
import io.cebes.spark.df.schema.SparkSchemaUtils
import io.cebes.spark.util.CebesSparkUtil
import org.apache.spark.sql.DataFrame

/**
  * Dataframe wrapper on top of Spark's DataFrame
  *
  * @param sparkDf the spark's DataFrame object
  */
class SparkDataframe(val sparkDf: DataFrame,
                     override val schema: Schema,
                     override val id: UUID) extends Dataframe {

  if (sparkDf.columns.length != schema.numCols) {
    throw new IllegalArgumentException(s"Invalid schema: schema has ${schema.numCols} columns," +
      s"while the data frame seems to has ${sparkDf.columns.length} columns")
  }

  def this(sparkDf: DataFrame) = {
    this(sparkDf, SparkSchemaUtils.getSchema(sparkDf), UUID.randomUUID())
  }

  def this(sparkDf: DataFrame, newSchema: Schema) = {
    this(sparkDf, newSchema, UUID.randomUUID())
  }

  /**
    * Number of rows
    *
    * @return a long
    */
  override def numRows: Long = sparkDf.count()

  /**
    * Automatically infer variable types, using various heuristics based on data
    *
    * @return the same [[Dataframe]]
    * @group Schema manipulation
    */
  override def inferVariableTypes(): Dataframe = {
    val sample = take(1000)
    schema.columns.zip(sample.data).foreach { case (c, data) =>
      c.setVariableType(SparkDataframe.inferVariableType(c.storageType, data))
    }
    this
  }

  /**
    * Manually update variable types for each column. Column names are case-insensitive.
    * Sanity checks will be performed. If new variable type doesn't conform with its storage type,
    * [[IllegalArgumentException]] will be thrown.
    *
    * @param newTypes map from column name -> new [[VariableType]]
    * @return the same [[Dataframe]]
    * @group Schema manipulation
    */
  override def updateVariableTypes(newTypes: Map[String, VariableType]): Dataframe = {
    newTypes.foreach { case (name, newType) =>
      schema.getColumnOptional(name).foreach { c =>
        if (!newType.validStorageTypes.contains(c.storageType)) {
          throw new IllegalArgumentException(s"Column ${c.name}: storage type ${c.storageType} cannot " +
            s"be casted as variable type $newType")
        }
      }
    }

    // no exception, we are good to go
    newTypes.foreach { case (name, newType) =>
      schema.getColumnOptional(name).foreach(_.setVariableType(newType))
    }
    this
  }

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
          if (!StorageTypes.values.contains(c.storageType)) {
            throw new IllegalArgumentException(s"Unrecognized storage type: ${c.storageType}")
          }
          r.get(idx)
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
    * Returns a new Dataframe sorted by the given expressions. This is an alias for `orderedBy`
    *
    * @group data-exploration
    */
  override def sort(sortExprs: Column*): Dataframe = ???

  /**
    * Returns a new Dataframe with columns dropped.
    * This is a no-op if schema doesn't contain column name(s).
    *
    * The colName string is treated literally without further interpretation.
    *
    * @group data-exploration
    */
  def drop(colNames: Seq[String]): Dataframe = {
    val droppedColNames = colNames.filter(schema.contains)
    if (droppedColNames.isEmpty) {
      this
    } else {
      new SparkDataframe(sparkDf.drop(droppedColNames: _*),
        schema.drop(droppedColNames))
    }
  }

  /**
    * Returns a new Dataframe that contains only the unique rows from this Dataframe.
    * This is an alias for [[distinct()]].
    *
    * Note that, equality checking is performed directly on the encoded representation of the data
    * and thus is not affected by a custom `equals` function.
    *
    * @group data-exploration
    */
  override def dropDuplicates(colNames: Seq[String]): Dataframe = {
    new SparkDataframe(sparkDf.dropDuplicates(colNames), schema.copy())
  }

  /**
    * SQL-like APIs
    */


  /**
    * Selects a set of columns based on expressions.
    *
    * @group sql-api
    */
  override def select(columns: Column*): Dataframe = ???

  /**
    * Filters rows using the given condition.
    *
    * @group sql-api
    */
  override def where(column: Column): Dataframe = ???

  /**
    * Returns a new Dataframe sorted by the given expressions. This is an alias for `sort`.
    *
    * @group sql-api
    */
  override def orderBy(sortExprs: Column*): Dataframe = ???

  /**
    * Selects column based on the column name and return it as a [[Column]].
    *
    * @group sql-api
    */
  override def col(colName: String): Column = ???

  /**
    * Returns a new Dataframe with an alias set.
    *
    * @group sql-api
    */
  override def alias(alias: String): Dataframe = {
    new SparkDataframe(sparkDf.alias(alias), schema.copy())
  }

  /**
    * Join with another [[Dataframe]], using the given join expression.
    *
    * {{{
    *   // Scala:
    *   df1.join(df2, df1.col("df1Key") === df2.col("df2Key"), "outer")
    * }}}
    *
    * @param right     Right side of the join.
    * @param joinExprs Join expression.
    * @param joinType  One of: `inner`, `outer`, `left_outer`, `right_outer`, `leftsemi`.
    * @group sql-api
    */
  override def join(right: Dataframe, joinExprs: Column, joinType: String): Dataframe = ???

  /**
    * Returns a new [[Dataframe]] by taking the first `n` rows.
    *
    * @group sql-api
    */
  override def limit(n: Int): Dataframe = {
    new SparkDataframe(sparkDf.limit(n), schema.copy())
  }

  /**
    * Returns a new Dataframe containing union of rows in this Dataframe and another Dataframe.
    *
    * To do a SQL-style set union (that does deduplication of elements), use this function followed
    * by a [[distinct]].
    *
    * @group sql-api
    */
  override def union(other: Dataframe): Dataframe = {
    val otherDf = CebesSparkUtil.getSparkDataframe(other).sparkDf
    new SparkDataframe(sparkDf.union(otherDf), schema.copy())
  }

  /**
    * Returns a new Dataframe containing rows only in both this Dataframe and another Dataframe.
    *
    * Note that, equality checking is performed directly on the encoded representation of the data
    * and thus is not affected by a custom `equals` function.
    *
    * @group sql-api
    */
  override def intersect(other: Dataframe): Dataframe = ???

  /**
    * Returns a new Dataframe containing rows in this Dataframe but not in another Dataframe.
    * This is equivalent to `EXCEPT` in SQL.
    *
    * Note that, equality checking is performed directly on the encoded representation of the data
    * and thus is not affected by a custom `equals` function.
    *
    * @group sql-api
    */
  override def except(other: Dataframe): Dataframe = ???

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
      sparkDf(currentCol.name).as(newCol.name).cast(SparkSchemaUtils.cebesTypesToSpark(newCol.storageType))
    }
    new SparkDataframe(sparkDf.select(sparkCols: _*))
  }
}

object SparkDataframe {

  private val UNIQUE_RATIO = 0.6

  def inferVariableType(storageType: StorageTypes.StorageType, sample: Seq[Any]): VariableType = {
    storageType match {
      case StorageTypes.BINARY | StorageTypes.VECTOR =>
        VariableTypes.fromStorageType(storageType)
      case StorageTypes.DATE | StorageTypes.TIMESTAMP =>
        VariableTypes.fromStorageType(storageType)
      case StorageTypes.BOOLEAN =>
        VariableTypes.fromStorageType(storageType)
      case StorageTypes.BYTE | StorageTypes.SHORT |
           StorageTypes.INT | StorageTypes.LONG =>
        val ratio = sample.distinct.length.toFloat / sample.length
        if (ratio > UNIQUE_RATIO) {
          VariableTypes.DISCRETE
        } else {
          VariableTypes.ORDINAL
        }
      case StorageTypes.FLOAT | StorageTypes.DOUBLE =>
        val ratio = sample.distinct.length.toFloat / sample.length
        if (ratio > UNIQUE_RATIO) {
          VariableTypes.CONTINUOUS
        } else {
          VariableTypes.ORDINAL
        }
      case StorageTypes.STRING =>
        val ratio = sample.distinct.length.toFloat / sample.length
        if (ratio > UNIQUE_RATIO) {
          VariableTypes.TEXT
        } else {
          VariableTypes.NOMINAL
        }
    }
  }
}
