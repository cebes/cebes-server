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
 * Created by phvu on 26/09/16.
 */

package io.cebes.spark.util

import com.typesafe.scalalogging.LazyLogging
import io.cebes.df.schema.{Schema, SchemaField}
import io.cebes.df.types.StorageTypes
import io.cebes.df.types.storage._
import org.apache.spark.sql.{DataFrame, types}
import org.apache.spark.ml.linalg

trait SparkSchemaUtils extends LazyLogging {

  /**
    * Extract the Schema from a Spark's DataFrame object
    *
    * @param sparkDf the Spark's DataFrame object
    * @return Schema object
    */
  def getSchema(sparkDf: DataFrame): Schema = {
    Schema(sparkDf.schema.map(f => new SchemaField(f.name, sparkTypesToCebes(f.dataType))).toArray)
  }

  /**
    * Create a new schema, based on the `originalSchema`, for the new Dataframe created from `sparkDf`
    * `newColumns` contains the possibly new columns in sparkDf that might not belong to originalSchema.
    * Normally this is used to infer the schema for the resulting Dataframe after transforming
    * another Dataframe (which has `originalSchema`).
    *
    * @return a new [[Schema]] object
    */
  def getSchema(sparkDf: DataFrame, originalSchema: Schema, newColumnNames: String*): Schema = {
    Schema(sparkDf.schema.map { f =>
      if (newColumnNames.contains(f.name) || originalSchema.get(f.name).isEmpty) {
        new SchemaField(f.name, sparkTypesToCebes(f.dataType))
      } else {
        originalSchema(f.name).copy()
      }
    }.toArray)
  }

  /**
    * Similar to [[getSchema(sparkDf: DataFrame, originalSchema: Schema, newColumnNames: String*)]],
    * but without the list of new column names.
    * This function will try its best to preserve the variable type.
    * If the new storage type is incompatible with the original schema, it will infer the
    * new storage type and variable type automatically.
    */
  def getSchema(sparkDf: DataFrame, originalSchema: Schema): Schema = {
    Schema(sparkDf.schema.fields.map { f =>
      originalSchema.get(f.name) match {
        case None =>
          new SchemaField(f.name, sparkTypesToCebes(f.dataType))
        case Some(sf) =>
          val newStorageType = sparkTypesToCebes(f.dataType)
          if (newStorageType == sf.storageType && sf.variableType.isValid(newStorageType)) {
            sf.copy(name = f.name)
          } else {
            new SchemaField(f.name, sparkTypesToCebes(f.dataType))
          }
      }
    })
  }

  /**
    * Convert from spark type to Cebes column type
    *
    * @param dt spark data type
    * @return cebes column type
    */
  def sparkTypesToCebes(dt: types.DataType): StorageType = dt match {
    case types.StringType => StorageTypes.StringType
    case types.BooleanType => StorageTypes.BooleanType
    case types.ByteType => StorageTypes.ByteType
    case types.ShortType => StorageTypes.ShortType
    case types.IntegerType => StorageTypes.IntegerType
    case types.LongType => StorageTypes.LongType
    case types.FloatType => StorageTypes.FloatType
    case types.DoubleType => StorageTypes.DoubleType
    case types.BinaryType => StorageTypes.BinaryType
    case types.DateType => StorageTypes.DateType
    case types.TimestampType => StorageTypes.TimestampType
    case types.CalendarIntervalType => StorageTypes.CalendarIntervalType
    case types.ArrayType(c, _) => StorageTypes.arrayType(sparkTypesToCebes(c))
    case types.MapType(c1, c2, _) => StorageTypes.mapType(sparkTypesToCebes(c1), sparkTypesToCebes(c2))
    case types.StructType(fields) => StorageTypes.structType(fields.map { f =>
      // TODO: preserve Metadata
      logger.warn(s"Any metadata in column ${f.name} will lost after the transformation")
      StorageTypes.structField(f.name, sparkTypesToCebes(f.dataType), Metadata.empty)
    })
    case t if t.typeName == "vector" => StorageTypes.VectorType
    case t => throw new IllegalArgumentException(s"Unrecognized spark type: ${t.toString}")
  }

  /**
    * Convert Cebes type to Spark DataType
    *
    * @param storageType Cebes column type
    * @return Spark data type
    */
  def cebesTypesToSpark(storageType: StorageType): types.DataType = storageType match {
    case StorageTypes.StringType => types.StringType
    case StorageTypes.BooleanType => types.BooleanType
    case StorageTypes.ByteType => types.ByteType
    case StorageTypes.ShortType => types.ShortType
    case StorageTypes.IntegerType => types.IntegerType
    case StorageTypes.LongType => types.LongType
    case StorageTypes.FloatType => types.FloatType
    case StorageTypes.DoubleType => types.DoubleType
    case StorageTypes.BinaryType => types.BinaryType
    case StorageTypes.DateType => types.DateType
    case StorageTypes.TimestampType => types.TimestampType
    case StorageTypes.CalendarIntervalType => types.CalendarIntervalType
    case ArrayType(c) => types.ArrayType(cebesTypesToSpark(c))
    case MapType(c1, c2) => types.MapType(cebesTypesToSpark(c1), cebesTypesToSpark(c2), valueContainsNull = true)
    case StructType(fields) => types.StructType(fields.map { f =>
      // TODO: preserve Metadata
      logger.warn(s"Any metadata in column ${f.name} will lost after the transformation")
      types.StructField(f.name, cebesTypesToSpark(f.storageType))
    })
    case t => throw new IllegalArgumentException(s"Unrecognized cebes type: ${t.toString}")
  }
}

object SparkSchemaUtils extends SparkSchemaUtils
