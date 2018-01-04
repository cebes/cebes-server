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
 * Created by phvu on 12/11/2016.
 */

package io.cebes.df.types

import io.cebes.df.types.storage.StructField

object StorageTypes {

  val StringType: storage.StringType = storage.StringType
  val BinaryType: storage.BinaryType = storage.BinaryType

  val DateType: storage.DateType = storage.DateType
  val TimestampType: storage.TimestampType = storage.TimestampType
  val CalendarIntervalType: storage.CalendarIntervalType = storage.CalendarIntervalType

  val BooleanType: storage.BooleanType = storage.BooleanType
  val ByteType: storage.ByteType = storage.ByteType
  val ShortType: storage.ShortType = storage.ShortType
  val IntegerType: storage.IntegerType = storage.IntegerType
  val LongType: storage.LongType = storage.LongType
  val FloatType: storage.FloatType = storage.FloatType
  val DoubleType: storage.DoubleType = storage.DoubleType

  /**
    * VectorType is not very well-designed, because Spark's VectorUDT
    * is still not stable yet.
    */
  val VectorType: storage.VectorType = storage.VectorType

  val atomicTypes = Seq(StringType, BinaryType,
    DateType, TimestampType, CalendarIntervalType,
    BooleanType, ByteType, ShortType, IntegerType,
    LongType, FloatType, DoubleType)

  def fromString(typeName: String): storage.StorageType = {
    typeName match {
      case s if s == VectorType.typeName => VectorType
      case _ =>
        atomicTypes.find(_.typeName.equalsIgnoreCase(typeName)) match {
          case Some(tp) => tp
          case _ => throw new IllegalArgumentException(s"Unrecognized storage type: $typeName")
        }
    }
  }

  def arrayType(elementType: storage.StorageType): storage.ArrayType = {
    storage.ArrayType(elementType)
  }

  def mapType(keyType: storage.StorageType, valueType: storage.StorageType): storage.MapType =
    storage.MapType(keyType, valueType)

  def structType(fields: Seq[StructField]): storage.StructType =
    storage.StructType(fields.toArray)

  def structType(field: StructField, fields: StructField*): storage.StructType =
    structType(field +: fields)

  def structField(name: String, storageType: storage.StorageType, metadata: storage.Metadata) =
    storage.StructField(name, storageType, metadata)
}
