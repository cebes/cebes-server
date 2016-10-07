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

package io.cebes.df.schema

object VariableTypes {

  sealed abstract class VariableType(val name: String, val isNumeric: Boolean, val isCategorical: Boolean) {
    override def toString: String = name
  }

  case object DISCRETE extends VariableType("Discrete", true, false)

  case object CONTINUOUS extends VariableType("Continuous", true, false)

  /**
    * Categorical variable without rank
    */
  case object NOMINAL extends VariableType("Nominal", false, true)

  /**
    * Categorical variable with a rank, an order
    */
  case object ORDINAL extends VariableType("Ordinal", false, true)

  case object TEXT extends VariableType("Text", false, false)

  case object DATETIME extends VariableType("DateTime", false, false)

  case object COMPLEX extends VariableType("Complex", false, false)

  val values = Seq(DISCRETE, CONTINUOUS, NOMINAL, ORDINAL, TEXT, COMPLEX)

  def fromString(name: String): VariableType = values.find(_.name == name) match {
    case Some(t) => t
    case None => throw new IllegalArgumentException(s"Unrecognized variable type: $name")
  }

  /**
    * Rude guess to infer variable type from storage type
    *
    * @param storageType storage type
    * @return variable types
    */
  def fromStorageType(storageType: StorageTypes.StorageType): VariableType = {
    storageType match {
      case StorageTypes.BINARY | StorageTypes.VECTOR =>
        VariableTypes.COMPLEX
      case StorageTypes.TIMESTAMP | StorageTypes.DATE  =>
        VariableTypes.DATETIME
      case StorageTypes.BOOLEAN => VariableTypes.NOMINAL
      case StorageTypes.BYTE | StorageTypes.SHORT |
           StorageTypes.INT | StorageTypes.LONG =>
        VariableTypes.DISCRETE
      case StorageTypes.FLOAT | StorageTypes.DOUBLE =>
        VariableTypes.CONTINUOUS
      case StorageTypes.STRING => VariableTypes.TEXT
    }
  }
}

