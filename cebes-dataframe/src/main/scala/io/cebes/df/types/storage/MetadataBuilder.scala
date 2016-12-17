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
 * Created by phvu on 17/12/2016.
 */

package io.cebes.df.types.storage

import scala.collection.mutable

/**
  * Builder for [[Metadata]]. If there is a key collision, the latter will overwrite the former.
  */
class MetadataBuilder {

  private val map: mutable.Map[String, Any] = mutable.Map.empty

  /** Include the content of an existing [[Metadata]] instance. */
  def withMetadata(metadata: Metadata): this.type = {
    map ++= metadata.map
    this
  }

  /** Puts a null. */
  def putNull(key: String): this.type = put(key, null)

  /** Puts a Long. */
  def putLong(key: String, value: Long): this.type = put(key, value)

  /** Puts a Double. */
  def putDouble(key: String, value: Double): this.type = put(key, value)

  /** Puts a Boolean. */
  def putBoolean(key: String, value: Boolean): this.type = put(key, value)

  /** Puts a String. */
  def putString(key: String, value: String): this.type = put(key, value)

  /** Puts a [[Metadata]]. */
  def putMetadata(key: String, value: Metadata): this.type = put(key, value)

  /** Puts a Long array. */
  def putLongArray(key: String, value: Array[Long]): this.type = put(key, value)

  /** Puts a Double array. */
  def putDoubleArray(key: String, value: Array[Double]): this.type = put(key, value)

  /** Puts a Boolean array. */
  def putBooleanArray(key: String, value: Array[Boolean]): this.type = put(key, value)

  /** Puts a String array. */
  def putStringArray(key: String, value: Array[String]): this.type = put(key, value)

  /** Puts a [[Metadata]] array. */
  def putMetadataArray(key: String, value: Array[Metadata]): this.type = put(key, value)

  /** Builds the [[Metadata]] instance. */
  def build(): Metadata = {
    new Metadata(map.toMap)
  }

  private def put(key: String, value: Any): this.type = {
    map.put(key, value)
    this
  }

  def remove(key: String): this.type = {
    map.remove(key)
    this
  }
}

