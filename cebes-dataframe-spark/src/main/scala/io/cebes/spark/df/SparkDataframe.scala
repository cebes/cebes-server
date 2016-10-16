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
import io.cebes.df.schema.{Schema, StorageTypes, VariableTypes}
import io.cebes.spark.df.schema.SparkSchemaUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataTypes

/**
  * Dataframe wrapper on top of Spark's DataFrame
  *
  * @param sparkDf the spark's DataFrame object
  */
class SparkDataframe(val sparkDf: DataFrame,
                     override val schema: Schema,
                     override val id: UUID) extends Dataframe {

  def this(sparkDf: DataFrame) = {
    this(sparkDf, SparkSchemaUtils.getSchema(sparkDf), UUID.randomUUID())
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
    schema.columns.zip(sample.columns).foreach { case (c, data) =>
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
