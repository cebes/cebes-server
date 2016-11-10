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

import io.cebes.common.ArgumentChecks
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
                     override val id: UUID) extends Dataframe with ArgumentChecks {

  checkArguments(sparkDf.columns.length == schema.numCols,
    s"Invalid schema: schema has ${schema.numCols} columns," +
      s"while the data frame seems to has ${sparkDf.columns.length} columns")

  def this(sparkDf: DataFrame) = {
    this(sparkDf, SparkSchemaUtils.getSchema(sparkDf), UUID.randomUUID())
  }

  def this(sparkDf: DataFrame, newSchema: Schema) = {
    this(sparkDf, newSchema, UUID.randomUUID())
  }

  override def numRows: Long = sparkDf.count()

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

  def sample(withReplacement: Boolean, fraction: Double, seed: Long): Dataframe = {
    new SparkDataframe(sparkDf.sample(withReplacement, fraction, seed))
  }

  override def createTempView(name: String) = {
    sparkDf.createTempView(name)
  }


  override def sort(sortExprs: Column*): Dataframe = ???

  def drop(colNames: Seq[String]): Dataframe = {
    val droppedColNames = colNames.filter(schema.contains)
    if (droppedColNames.isEmpty) {
      this
    } else {
      new SparkDataframe(sparkDf.drop(droppedColNames: _*),
        schema.drop(droppedColNames))
    }
  }

  override def dropDuplicates(colNames: Seq[String]): Dataframe = {
    new SparkDataframe(sparkDf.dropDuplicates(colNames), schema.copy())
  }

  /**
    * SQL-like APIs
    */

  override def select(columns: Column*): Dataframe = ???

  override def where(column: Column): Dataframe = ???

  override def orderBy(sortExprs: Column*): Dataframe = ???

  override def col(colName: String): Column = colName match {
    case "*" =>
      throw new NotImplementedError("")
    case _ =>
      checkArguments(schema.contains(colName), s"Column name not found: $colName")
      schema.getColumn(colName)
  }

  override def alias(alias: String): Dataframe = {
    new SparkDataframe(sparkDf.alias(alias), schema.copy())
  }

  override def join(right: Dataframe, joinExprs: Column, joinType: String): Dataframe = ???

  override def limit(n: Int): Dataframe = {
    checkArguments(n >= 0, s"The limit must be equal to or greater than 0, but got $n")
    new SparkDataframe(sparkDf.limit(n), schema.copy())
  }

  override def union(other: Dataframe): Dataframe = {
    checkArguments(other.numCols == numCols,
      s"Unions only work for tables with the same number of columns, " +
        s"but got ${this.numCols} and ${other.numCols} columns respectively")
    val otherDf = CebesSparkUtil.getSparkDataframe(other).sparkDf
    new SparkDataframe(sparkDf.union(otherDf), schema.copy())
  }

  override def intersect(other: Dataframe): Dataframe = {
    checkArguments(other.numCols == numCols,
      s"Intersects only work for tables with the same number of columns, " +
        s"but got ${this.numCols} and ${other.numCols} columns respectively")
    val otherDf = CebesSparkUtil.getSparkDataframe(other).sparkDf
    new SparkDataframe(sparkDf.intersect(otherDf), schema.copy())
  }

  override def except(other: Dataframe): Dataframe = {
    checkArguments(other.numCols == numCols,
      s"Excepts only work for tables with the same number of columns, " +
        s"but got ${this.numCols} and ${other.numCols} columns respectively")
    val otherDf = CebesSparkUtil.getSparkDataframe(other).sparkDf
    new SparkDataframe(sparkDf.except(otherDf), schema.copy())
  }

  override def applySchema(newSchema: Schema): Dataframe = {
    checkArguments(schema.numCols == newSchema.numCols,
      s"Incompatible schema: current schema has ${schema.numCols} columns," +
        s" but the new schema has ${newSchema.numCols} columns")

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
