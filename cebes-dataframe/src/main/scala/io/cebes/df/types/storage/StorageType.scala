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
 * Created by phvu on 10/11/2016.
 */

package io.cebes.df.types.storage

/**
  * Mother of storage types
  */
trait StorageType {

  def typeName: String = {
    this.getClass.getSimpleName.stripSuffix("$").stripSuffix("Type").toLowerCase
  }
}

/**
  *
  */

class StringType private() extends StorageType {
}
case object StringType extends StringType

class BinaryType private() extends StorageType {
}
case object BinaryType extends BinaryType

/**
  *
  */

class DateType private() extends StorageType {
}
case object DateType extends DateType

class TimestampType private() extends StorageType {
}
case object TimestampType extends TimestampType

class CalendarIntervalType private() extends StorageType {
}
case object CalendarIntervalType extends CalendarIntervalType

/**
  *
  */

class BooleanType private() extends StorageType {
}
case object BooleanType extends BooleanType

class ByteType private() extends StorageType {
}
case object ByteType extends ByteType

class ShortType private() extends StorageType {
}
case object ShortType extends ShortType

class IntegerType private() extends StorageType {
}
case object IntegerType extends IntegerType

class LongType private() extends StorageType {
}
case object LongType extends LongType

class FloatType private() extends StorageType {
}
case object FloatType extends FloatType

class DoubleType private() extends StorageType {
}
case object DoubleType extends DoubleType

class VectorType private() extends StorageType {
}
case object VectorType extends VectorType

/**
  *
  */
case class ArrayType(elementType: StorageType) extends StorageType

case class MapType(keyType: StorageType, valueType: StorageType) extends StorageType

case class StructField(name: String, storageType: StorageType, metadata: Metadata = Metadata.empty)

case class StructType(fields: Array[StructField]) extends StorageType with Seq[StructField] {

  override def length: Int = fields.length

  override def iterator: Iterator[StructField] = fields.iterator

  override def apply(fieldIndex: Int): StructField = fields(fieldIndex)

  /**
    * Return all field names in an array
    */
  def fieldNames: Array[String] = fields.map(_.name)

  /**
    * Creates a new [[StructType]] by adding a new field.
    * {{{
    * val struct = (new StructType)
    *   .add(StructField("a", IntegerType))
    *   .add(StructField("b", LongType))
    *}}}
    */
  def add(field: StructField): StructType = StructType(fields :+ field)

  /**
    * Creates a new [[StructType]] by adding a new field.
    * {{{
    * val struct = (new StructType)
    *   .add("a", IntegerType)
    *   .add("b", LongType)
    *}}}
    */
  def add(fieldName: String, storageType: StorageType): StructType = add(StructField(fieldName, storageType))
}

