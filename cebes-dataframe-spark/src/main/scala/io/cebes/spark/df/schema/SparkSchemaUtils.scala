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

package io.cebes.spark.df.schema

import io.cebes.df.schema.{Schema, SchemaField}
import io.cebes.df.types.StorageTypes
import io.cebes.df.types.storage.StorageType
import org.apache.spark.sql.{DataFrame, types}

object SparkSchemaUtils {

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
    case types.ArrayType(types.DoubleType, true) => StorageTypes.VectorType
    case types.ArrayType(c, _) => StorageTypes.arrayType(sparkTypesToCebes(c))
    case types.BinaryType => StorageTypes.BinaryType
    case types.DateType => StorageTypes.DateType
    case types.TimestampType => StorageTypes.TimestampType
    case types.CalendarIntervalType => StorageTypes.CalendarIntervalType
    case t => throw new IllegalArgumentException(s"Unrecognized spark type: ${t.simpleString}")
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
    case StorageTypes.VectorType => types.ArrayType(types.DoubleType)
    case StorageTypes.BinaryType => types.BinaryType
    case StorageTypes.DateType => types.DateType
    case StorageTypes.TimestampType => types.TimestampType
    case StorageTypes.CalendarIntervalType => types.CalendarIntervalType
    case t => throw new IllegalArgumentException(s"Unrecognized cebes type: ${t.toString}")
  }

}
