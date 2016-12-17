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
 * Created by phvu on 06/10/16.
 */

package io.cebes.df.types

import io.cebes.df.types.storage.StorageType

object VariableTypes {

  sealed abstract class VariableType(val name: String, val isNumeric: Boolean,
                                     val isCategorical: Boolean,
                                     val validStorageTypes: Seq[StorageType],
                                     val validStorageClasses: Option[Seq[Class[_ <: StorageType]]] = None) {
    override def toString: String = name

    def isValid(storageType: StorageType): Boolean = {
      validStorageTypes.contains(storageType) || (validStorageClasses.nonEmpty &&
        validStorageClasses.get.exists(_.isInstance(storageType)))
    }
  }

  case object DISCRETE extends VariableType("Discrete", true, false,
    Seq(StorageTypes.ByteType, StorageTypes.ShortType, StorageTypes.IntegerType, StorageTypes.LongType))

  case object CONTINUOUS extends VariableType("Continuous", true, false,
    Seq(StorageTypes.FloatType, StorageTypes.DoubleType))

  /**
    * Categorical variable without rank
    */
  case object NOMINAL extends VariableType("Nominal", false, true,
    Seq(StorageTypes.StringType, StorageTypes.BooleanType,
      StorageTypes.ByteType, StorageTypes.ShortType, StorageTypes.IntegerType, StorageTypes.LongType,
      StorageTypes.FloatType, StorageTypes.DoubleType,
      StorageTypes.DateType, StorageTypes.TimestampType))

  /**
    * Categorical variable with a rank, an order
    */
  case object ORDINAL extends VariableType("Ordinal", false, true,
    Seq(StorageTypes.StringType, StorageTypes.BooleanType,
      StorageTypes.ByteType, StorageTypes.ShortType, StorageTypes.IntegerType, StorageTypes.LongType,
      StorageTypes.FloatType, StorageTypes.DoubleType,
      StorageTypes.DateType, StorageTypes.TimestampType))

  case object TEXT extends VariableType("Text", false, false,
    Seq(StorageTypes.StringType))

  case object DATETIME extends VariableType("DateTime", false, false,
    Seq(StorageTypes.DateType, StorageTypes.TimestampType, StorageTypes.CalendarIntervalType))

  case object ARRAY extends VariableType("Array", false, false,
    Seq(StorageTypes.BinaryType),
    Some(Seq(classOf[storage.ArrayType])))

  case object MAP extends VariableType("Map", false, false,
    Seq(), Some(Seq(classOf[storage.MapType])))

  case object STRUCT extends VariableType("Struct", false, false,
    Seq(), Some(Seq(classOf[storage.StructType])))

  val values = Seq(DISCRETE, CONTINUOUS, NOMINAL, ORDINAL, TEXT, DATETIME, ARRAY, MAP, STRUCT)

  def fromString(name: String): Option[VariableType] = values.find(_.name == name)

  /**
    * Rude guess to infer variable type from storage type
    *
    * @param storageType storage type
    * @return variable types
    */
  def fromStorageType(storageType: StorageType): VariableType = {
    storageType match {
      case StorageTypes.BinaryType =>
        VariableTypes.ARRAY
      case StorageTypes.TimestampType | StorageTypes.DateType | StorageTypes.CalendarIntervalType =>
        VariableTypes.DATETIME
      case StorageTypes.BooleanType =>
        VariableTypes.NOMINAL
      case StorageTypes.ByteType | StorageTypes.ShortType |
           StorageTypes.IntegerType | StorageTypes.LongType =>
        VariableTypes.DISCRETE
      case StorageTypes.FloatType | StorageTypes.DoubleType =>
        VariableTypes.CONTINUOUS
      case StorageTypes.StringType =>
        VariableTypes.TEXT
      case _: storage.ArrayType =>
        VariableTypes.ARRAY
      case _: storage.MapType =>
        VariableTypes.MAP
      case _: storage.StructType =>
        VariableTypes.STRUCT
    }
  }
}

