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

import io.cebes.df.schema.{Column, ColumnTypes, Schema}
import org.apache.spark.sql.{DataFrame, types}

object SparkSchemaUtils {

  /**
    * Extract the Schema from a Spark's DataFrame object
    *
    * @param sparkDf the Spark's DataFrame object
    * @return Schema object
    */
  def getSchema(sparkDf: DataFrame): Schema = {
    new Schema(sparkDf.schema.map(f => Column(f.name, sparkTypesToCebes(f.dataType))))
  }

  /**
    * Convert from spark type to Cebes column type
    *
    * @param dt spark data type
    * @return cebes column type
    */
  def sparkTypesToCebes(dt: types.DataType): ColumnTypes.ColumnType = dt match {
    case types.StringType => ColumnTypes.STRING
    case types.BooleanType => ColumnTypes.BOOLEAN
    case types.ByteType => ColumnTypes.BYTE
    case types.ShortType => ColumnTypes.SHORT
    case types.IntegerType => ColumnTypes.INT
    case types.LongType => ColumnTypes.LONG
    case types.FloatType => ColumnTypes.FLOAT
    case types.DoubleType => ColumnTypes.DOUBLE
    case types.ArrayType(types.FloatType, true) => ColumnTypes.VECTOR
    case types.BinaryType => ColumnTypes.BINARY
    case types.DateType => ColumnTypes.DATE
    case types.TimestampType => ColumnTypes.TIMESTAMP
    case t => throw new IllegalArgumentException(s"Unrecognized spark type: ${t.simpleString}")
  }

  /**
    * Convert Cebes type to Spark DataType
    *
    * @param colType Cebes column type
    * @return Spark data type
    */
  def cebesTypesToSpark(colType: ColumnTypes.ColumnType): types.DataType = colType match {
    case ColumnTypes.STRING => types.StringType
    case ColumnTypes.BOOLEAN => types.BooleanType
    case ColumnTypes.BYTE => types.ByteType
    case ColumnTypes.SHORT => types.ShortType
    case ColumnTypes.INT => types.IntegerType
    case ColumnTypes.LONG => types.LongType
    case ColumnTypes.FLOAT => types.FloatType
    case ColumnTypes.DOUBLE => types.DoubleType
    case ColumnTypes.VECTOR => types.ArrayType(types.FloatType)
    case ColumnTypes.BINARY => types.BinaryType
    case ColumnTypes.DATE => types.DateType
    case ColumnTypes.TIMESTAMP => types.TimestampType
    case t => throw new IllegalArgumentException(s"Unrecognized cebes type: ${t.toString}")
  }

}
