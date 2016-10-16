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

import io.cebes.df.schema.{Column, Schema, StorageTypes}
import org.apache.spark.sql.{DataFrame, types}

object SparkSchemaUtils {

  /**
    * Extract the Schema from a Spark's DataFrame object
    *
    * @param sparkDf the Spark's DataFrame object
    * @return Schema object
    */
  def getSchema(sparkDf: DataFrame): Schema = {
    new Schema(sparkDf.schema.map(f => new Column(f.name, sparkTypesToCebes(f.dataType))))
  }

  /**
    * Convert from spark type to Cebes column type
    *
    * @param dt spark data type
    * @return cebes column type
    */
  def sparkTypesToCebes(dt: types.DataType): StorageTypes.StorageType = dt match {
    case types.StringType => StorageTypes.STRING
    case types.BooleanType => StorageTypes.BOOLEAN
    case types.ByteType => StorageTypes.BYTE
    case types.ShortType => StorageTypes.SHORT
    case types.IntegerType => StorageTypes.INT
    case types.LongType => StorageTypes.LONG
    case types.FloatType => StorageTypes.FLOAT
    case types.DoubleType => StorageTypes.DOUBLE
    case types.ArrayType(types.DoubleType, true) => StorageTypes.VECTOR
    case types.BinaryType => StorageTypes.BINARY
    case types.DateType => StorageTypes.DATE
    case types.TimestampType => StorageTypes.TIMESTAMP
    case t => throw new IllegalArgumentException(s"Unrecognized spark type: ${t.simpleString}")
  }

  /**
    * Convert Cebes type to Spark DataType
    *
    * @param storageType Cebes column type
    * @return Spark data type
    */
  def cebesTypesToSpark(storageType: StorageTypes.StorageType): types.DataType = storageType match {
    case StorageTypes.STRING => types.StringType
    case StorageTypes.BOOLEAN => types.BooleanType
    case StorageTypes.BYTE => types.ByteType
    case StorageTypes.SHORT => types.ShortType
    case StorageTypes.INT => types.IntegerType
    case StorageTypes.LONG => types.LongType
    case StorageTypes.FLOAT => types.FloatType
    case StorageTypes.DOUBLE => types.DoubleType
    case StorageTypes.VECTOR => types.ArrayType(types.DoubleType)
    case StorageTypes.BINARY => types.BinaryType
    case StorageTypes.DATE => types.DateType
    case StorageTypes.TIMESTAMP => types.TimestampType
    case t => throw new IllegalArgumentException(s"Unrecognized cebes type: ${t.toString}")
  }

}
