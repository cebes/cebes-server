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
 * Created by phvu on 14/11/2016.
 */

package io.cebes.df.schema

import io.cebes.df.types.VariableTypes
import io.cebes.df.types.VariableTypes.VariableType
import io.cebes.df.types.storage.StorageType

case class SchemaField(name: String, storageType: StorageType, variableType: VariableType) {

  if (!variableType.validStorageTypes.contains(storageType)) {
    throw new IllegalArgumentException(s"Invalid variable type: column $name has storage type $storageType " +
      s"but forced to have variable type $variableType.")
  }

  def this(name: String, storageType: StorageType) {
    this(name, storageType, VariableTypes.fromStorageType(storageType))
  }

}

case class Schema(fields: Array[SchemaField] = Array.empty) extends Seq[SchemaField] {

  override def length: Int = fields.length

  override def iterator: Iterator[SchemaField] = fields.iterator

  override def apply(fieldIndex: Int): SchemaField = fields(fieldIndex)

  def apply(name: String): SchemaField = {
    get(name).getOrElse(throw new IllegalArgumentException(s"""Field "$name" does not exist."""))
  }

  def fieldNames = fields.map(_.name)

  /**
    * Get the field with the given name (case-insensitive)
    */
  def get(name: String): Option[SchemaField] = find(_.name.equalsIgnoreCase(name))

  /**
    * Whether this schema contains a field with the given name (case-insensitive)
    */
  def contains(name: String): Boolean = get(name).isDefined

  /**
    * Return a new [[Schema]] without the given field
    */
  def remove(names: Seq[String]): Schema = {
    Schema(fields.filterNot(f => names.exists(_.equalsIgnoreCase(f.name))).map(_.copy()))
  }

  /**
    * Return a new [[Schema]] without the given field
    */
  def remove(name: String): Schema = remove(Seq(name))

  /**
    * Creates a new [[Schema]] by adding a new field.
    * {{{
    * val schema = Schema()
    *   .add(SchemaField("a", IntegerType))
    *   .add(SchemaField("b", LongType))
    * }}}
    */
  def add(field: SchemaField): Schema = Schema(fields :+ field)

  /**
    * Creates a new [[Schema]] by adding a new field.
    * {{{
    * val schema = Schema()
    *   .add("a", IntegerType)
    *   .add("b", LongType)
    * }}}
    */
  def add(name: String, storageType: StorageType): Schema = add(new SchemaField(name, storageType))

  /**
    * Creates a new [[Schema]] by adding a new field.
    * {{{
    * val schema = Schema()
    *   .add("a", IntegerType, VariableTypes.NOMIAL)
    *   .add("b", LongType, , VariableTypes.NOMIAL)
    * }}}
    */
  def add(name: String, storageType: StorageType, variableType: VariableType): Schema =
  add(SchemaField(name, storageType, variableType))
}
