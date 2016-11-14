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
import io.cebes.df.expressions.Column
import io.cebes.df.sample.DataSample
import io.cebes.df.types.VariableTypes.VariableType
import io.cebes.df.schema.Schema
import io.cebes.df.types.{StorageTypes, VariableTypes}
import io.cebes.df.types.storage.StorageType
import io.cebes.spark.df.schema.SparkSchemaUtils
import io.cebes.spark.util.CebesSparkUtil
import org.apache.spark.sql.DataFrame

/**
  * Dataframe wrapper on top of Spark's DataFrame
  *
  * @param sparkDf the spark's DataFrame object
  */
class SparkDataframe(val sparkDf: DataFrame, val schema: Schema, val id: UUID) extends Dataframe with ArgumentChecks {

  def this(sparkDf: DataFrame, schema: Schema) = {
    this(sparkDf, schema, UUID.randomUUID())
  }

  def this(sparkDf: DataFrame) = {
    this(sparkDf, SparkSchemaUtils.getSchema(sparkDf), UUID.randomUUID())
  }

  override def numRows: Long = sparkDf.count()

  ////////////////////////////////////////////////////////////////////////////////////
  // Variable types
  ////////////////////////////////////////////////////////////////////////////////////

  override def inferVariableTypes(): Dataframe = {
    val sample = take(1000)
    val fields = schema.zip(sample.data).map { case (c, data) =>
      c.copy(variableType = SparkDataframe.inferVariableType(c.storageType, data))
    }.toArray
    new SparkDataframe(sparkDf, Schema(fields))
  }

  override def updateVariableTypes(newTypes: Map[String, VariableType]): Dataframe = {
    val fields = schema.map { f =>
      newTypes.find(_._1.equalsIgnoreCase(f.name)).map(_._2) match {
        case Some(n) if n.validStorageTypes.contains(f.storageType) => f.copy(variableType = n)
        case Some(n) => throw new IllegalArgumentException(s"Column ${f.name} has storage type ${f.storageType} " +
          s"but is assigned variable type $n")
        case None => f.copy()
      }
    }
    new SparkDataframe(sparkDf, Schema(fields.toArray))
  }

  override def applySchema(newSchema: Schema): Dataframe = {
    checkArguments(schema.length == newSchema.length,
      s"Incompatible schema: current schema has ${schema.length} columns," +
        s" but the new schema has ${newSchema.length} columns")

    val sparkCols = schema.fields.zip(newSchema.fields).map { case (currentCol, newCol) =>
      sparkDf(currentCol.name).as(newCol.name).cast(SparkSchemaUtils.cebesTypesToSpark(newCol.storageType))
    }
    new SparkDataframe(sparkDf.select(sparkCols: _*), newSchema)
  }

  ////////////////////////////////////////////////////////////////////////////////////
  // Sampling
  ////////////////////////////////////////////////////////////////////////////////////

  def take(n: Int = 1): DataSample = {
    val rows = sparkDf.take(n)
    val cols = schema.fieldNames.zipWithIndex.map {
      case (c, idx) => rows.map(_.get(idx)).toSeq
    }
    new DataSample(schema.copy(), cols)
  }

  def sample(withReplacement: Boolean, fraction: Double, seed: Long): Dataframe = {
    new SparkDataframe(sparkDf.sample(withReplacement, fraction, seed), schema.copy())
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
      new SparkDataframe(sparkDf.drop(droppedColNames: _*), schema.remove(colNames))
    }
  }

  override def dropDuplicates(colNames: Seq[String]): Dataframe = {
    new SparkDataframe(sparkDf.dropDuplicates(colNames), schema.copy())
  }

  ////////////////////////////////////////////////////////////////////////////////////
  // SQL-like APIs
  ////////////////////////////////////////////////////////////////////////////////////

  override def select(columns: Column*): Dataframe = ???

  override def where(column: Column): Dataframe = ???

  override def orderBy(sortExprs: Column*): Dataframe = ???

  override def col(colName: String): Column = colName match {
    case "*" =>
      throw new NotImplementedError("")
    case _ =>
      checkArguments(schema.contains(colName), s"Column name not found: $colName")
      throw new NotImplementedError("")
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
}

object SparkDataframe {

  private val UNIQUE_RATIO = 0.6

  def inferVariableType(storageType: StorageType, sample: Seq[Any]): VariableType = {
    storageType match {
      case StorageTypes.BinaryType | StorageTypes.VectorType =>
        VariableTypes.fromStorageType(storageType)
      case StorageTypes.DateType | StorageTypes.TimestampType | StorageTypes.CalendarIntervalType =>
        VariableTypes.fromStorageType(storageType)
      case StorageTypes.BooleanType =>
        VariableTypes.fromStorageType(storageType)
      case StorageTypes.ByteType | StorageTypes.ShortType |
           StorageTypes.IntegerType | StorageTypes.LongType =>
        val ratio = sample.distinct.length.toFloat / sample.length
        if (ratio > UNIQUE_RATIO) {
          VariableTypes.DISCRETE
        } else {
          VariableTypes.ORDINAL
        }
      case StorageTypes.FloatType | StorageTypes.DoubleType =>
        val ratio = sample.distinct.length.toFloat / sample.length
        if (ratio > UNIQUE_RATIO) {
          VariableTypes.CONTINUOUS
        } else {
          VariableTypes.ORDINAL
        }
      case StorageTypes.StringType =>
        val ratio = sample.distinct.length.toFloat / sample.length
        if (ratio > UNIQUE_RATIO) {
          VariableTypes.TEXT
        } else {
          VariableTypes.NOMINAL
        }
    }
  }
}
