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

import io.cebes.df.sample.DataSample
import io.cebes.df.schema.Schema
import io.cebes.df.support.{GroupedDataframe, NAFunctions, StatFunctions}
import io.cebes.df.types.VariableTypes.VariableType
import io.cebes.df.types.storage.StorageType
import io.cebes.df.types.{StorageTypes, VariableTypes}
import io.cebes.df.{Column, Dataframe}
import io.cebes.spark.df.expressions.{SparkExpressionParser, SparkPrimitiveExpression}
import io.cebes.spark.df.schema.SparkSchemaUtils
import io.cebes.spark.df.support.{SparkGroupedDataframe, SparkNAFunctions, SparkStatFunctions}
import io.cebes.spark.util.CebesSparkUtil
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{DataFrame, functions => sparkFunctions}

/**
  * Dataframe wrapper on top of Spark's DataFrame
  *
  * Note that we don't (and WON'T) inject [[io.cebes.df.store.DataframeStore]] into this class,
  * because the logic of handling a bunch of [[Dataframe]]s should be at the higher level,
  * specifically the level of [[io.cebes.df.DataframeService]]
  *
  * Don't initialize this class directly. Use [[SparkDataframeFactory]] instead.
  *
  * @param sparkDf the spark's DataFrame object
  */
class SparkDataframe private[df](private val dfFactory: SparkDataframeFactory,
                                 private val parser: SparkExpressionParser,
                                 val sparkDf: DataFrame,
                                 val schema: Schema,
                                 val id: UUID) extends Dataframe with CebesSparkUtil {

  require(sparkDf.columns.length == schema.length &&
    sparkDf.columns.zip(schema).forall(t => t._2.compareName(t._1)),
    s"Invalid schema: schema has ${schema.length} columns (${schema.fieldNames.mkString(", ")})," +
      s"while the data frame seems to has ${sparkDf.columns.length} columns (${sparkDf.columns.mkString(", ")})")

  override def numRows: Long = sparkDf.count()

  ////////////////////////////////////////////////////////////////////////////////////
  // Variable types
  ////////////////////////////////////////////////////////////////////////////////////

  override def inferVariableTypes(sampleSize: Int = 1000): Dataframe = {
    val sample = take(sampleSize)
    val fields = schema.zip(sample.data).map { case (c, data) =>
      c.copy(variableType = SparkDataframe.inferVariableType(c.storageType, data))
    }.toArray
    withSparkDataFrame(sparkDf, Schema(fields))
  }

  override def withVariableTypes(newTypes: Map[String, VariableType]): Dataframe = {
    val fields = schema.map { f =>
      newTypes.find(entry => f.compareName(entry._1)).map(_._2) match {
        case Some(n) if n.isValid(f.storageType) => f.copy(variableType = n)
        case Some(n) => throw new IllegalArgumentException(s"Column ${f.name} has storage type ${f.storageType} " +
          s"but is assigned variable type $n")
        case None => f.copy()
      }
    }
    withSparkDataFrame(sparkDf, Schema(fields.toArray))
  }

  override def applySchema(newSchema: Schema): Dataframe = {
    require(schema.length == newSchema.length,
      s"Incompatible schema: current schema has ${schema.length} columns," +
        s" but the new schema has ${newSchema.length} columns")

    val sparkCols = schema.fields.zip(newSchema.fields).map { case (currentCol, newCol) =>
      sparkDf(currentCol.name).as(newCol.name).cast(SparkSchemaUtils.cebesTypesToSpark(newCol.storageType))
    }
    withSparkDataFrame(sparkDf.select(sparkCols: _*), newSchema)
  }

  ////////////////////////////////////////////////////////////////////////////////////
  // Sampling
  ////////////////////////////////////////////////////////////////////////////////////

  def take(n: Int = 1): DataSample = {

    def getValue(v: Any): Any = v match {
      case r: GenericRowWithSchema =>
        r.schema.fields.map { f =>
          f.name -> getValue(r.get(r.fieldIndex(f.name)))
        }.toMap
      case other => other
    }

    val rows = sparkDf.take(n)
    val cols = schema.fieldNames.zipWithIndex.map {
      case (_, idx) => rows.map(row => getValue(row.get(idx))).toSeq
    }
    new DataSample(schema.copy(), cols)
  }

  def sample(withReplacement: Boolean, fraction: Double, seed: Long): Dataframe = {
    withSparkDataFrame(sparkDf.sample(withReplacement, fraction, seed), schema.copy())
  }

  override def createTempView(name: String): Unit = {
    sparkDf.createTempView(name)
  }

  ////////////////////////////////////////////////////////////////////////////////////
  // Data exploration
  ////////////////////////////////////////////////////////////////////////////////////

  override def sort(sortExprs: Column*): Dataframe =
    withSparkDataFrame(sparkDf.sort(parser.toSpark(sortExprs): _*), schema.copy())

  def drop(colNames: Seq[String]): Dataframe = {
    val droppedColNames = colNames.filter(schema.contains)
    if (droppedColNames.isEmpty) {
      this
    } else {
      withSparkDataFrame(sparkDf.drop(droppedColNames: _*), schema.remove(colNames))
    }
  }

  override def dropDuplicates(colNames: Seq[String]): Dataframe = {
    withSparkDataFrame(sparkDf.dropDuplicates(colNames), schema.copy())
  }

  override def na: NAFunctions = new SparkNAFunctions(dfFactory, safeSparkCall(sparkDf.na))

  override def stat: StatFunctions = new SparkStatFunctions(dfFactory, safeSparkCall(sparkDf.stat))

  ////////////////////////////////////////////////////////////////////////////////////
  // SQL-like APIs
  ////////////////////////////////////////////////////////////////////////////////////

  override def withColumn(colName: String, col: Column): Dataframe = {
    val newSparkDf = safeSparkCall(sparkDf.withColumn(colName, parser.toSpark(col)))
    withSparkDataFrame(newSparkDf, schema.withField(colName,
      SparkSchemaUtils.sparkTypesToCebes(newSparkDf.schema(colName).dataType)))
  }

  override def withColumnRenamed(existingName: String, newName: String): Dataframe = {
    withSparkDataFrame(sparkDf.withColumnRenamed(existingName, newName),
      schema.withFieldRenamed(existingName, newName))
  }

  override def select(columns: Column*): Dataframe = {
    //TODO: preserve custom information in schema
    withSparkDataFrame(sparkDf.select(parser.toSpark(columns): _*))
  }

  override def select(col: String, cols: String*): Dataframe = select((col +: cols).map(this.col): _*)

  override def where(column: Column): Dataframe = {
    //TODO: preserve custom information in schema
    withSparkDataFrame(sparkDf.where(parser.toSpark(column)))
  }

  override def col(colName: String): Column = {
    new Column(SparkPrimitiveExpression(this.id, colName,
      Some(safeSparkCall(sparkDf.col(colName)))))
  }

  override def alias(alias: String): Dataframe = {
    withSparkDataFrame(sparkDf.alias(alias), schema.copy())
  }

  override def join(right: Dataframe, joinExprs: Column, joinType: String): Dataframe = {
    //TODO: preserve custom information in schema
    val rightDf = getSparkDataframe(right).sparkDf
    withSparkDataFrame(sparkDf.join(rightDf, parser.toSpark(joinExprs), joinType))
  }

  override def limit(n: Int): Dataframe = {
    require(n >= 0, s"The limit must be equal to or greater than 0, but got $n")
    withSparkDataFrame(sparkDf.limit(n), schema.copy())
  }

  override def union(other: Dataframe): Dataframe = {
    require(other.numCols == numCols,
      s"Unions only work for tables with the same number of columns, " +
        s"but got ${this.numCols} and ${other.numCols} columns respectively")
    val otherDf = getSparkDataframe(other).sparkDf
    withSparkDataFrame(sparkDf.union(otherDf), schema.copy())
  }

  override def intersect(other: Dataframe): Dataframe = {
    require(other.numCols == numCols,
      s"Intersects only work for tables with the same number of columns, " +
        s"but got ${this.numCols} and ${other.numCols} columns respectively")
    val otherDf = getSparkDataframe(other).sparkDf
    withSparkDataFrame(sparkDf.intersect(otherDf), schema.copy())
  }

  override def except(other: Dataframe): Dataframe = {
    require(other.numCols == numCols,
      s"Excepts only work for tables with the same number of columns, " +
        s"but got ${this.numCols} and ${other.numCols} columns respectively")
    val otherDf = getSparkDataframe(other).sparkDf
    withSparkDataFrame(sparkDf.except(otherDf), schema.copy())
  }

  override def broadcast: Dataframe = withSparkDataFrame(sparkFunctions.broadcast(sparkDf), schema.copy())

  ////////////////////////////////////////////////////////////////////////////////////
  // GroupBy-related functions
  ////////////////////////////////////////////////////////////////////////////////////

  override def groupBy(cols: Column*): GroupedDataframe = {
    new SparkGroupedDataframe(dfFactory, parser, safeSparkCall(sparkDf.groupBy(parser.toSpark(cols): _*)))
  }

  override def rollup(cols: Column*): GroupedDataframe = {
    new SparkGroupedDataframe(dfFactory, parser, safeSparkCall(sparkDf.rollup(parser.toSpark(cols): _*)))
  }

  override def cube(cols: Column*): GroupedDataframe = {
    new SparkGroupedDataframe(dfFactory, parser, safeSparkCall(sparkDf.cube(parser.toSpark(cols): _*)))
  }

  ////////////////////////////////////////////////////////////////////////////////////
  // Private helpers
  ////////////////////////////////////////////////////////////////////////////////////

  /**
    * short-hand for returning a SparkDataframe, with proper exception handling
    */
  private def withSparkDataFrame(df: => DataFrame): Dataframe = {
    dfFactory.df(safeSparkCall(df))
  }

  /**
    * short-hand for returning a SparkDataframe, with proper exception handling
    */
  private def withSparkDataFrame(df: => DataFrame, schema: Schema): Dataframe = {
    dfFactory.df(safeSparkCall(df), schema)
  }
}

object SparkDataframe {

  private val UNIQUE_RATIO = 0.6

  def inferVariableType(storageType: StorageType, sample: Seq[Any]): VariableType = {
    storageType match {
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
      case t => VariableTypes.fromStorageType(t)
    }
  }
}
