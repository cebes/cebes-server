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

package io.cebes.df.types.storage

import scala.collection.mutable

class Metadata(private[storage] val map: Map[String, Any]) {

  /** Tests whether this Metadata contains a binding for a key. */
  def contains(key: String): Boolean = map.contains(key)

  /** Gets a Long. */
  def getLong(key: String): Long = get(key)

  /** Gets a Double. */
  def getDouble(key: String): Double = get(key)

  /** Gets a Boolean. */
  def getBoolean(key: String): Boolean = get(key)

  /** Gets a String. */
  def getString(key: String): String = get(key)

  /** Gets a Metadata. */
  def getMetadata(key: String): Metadata = get(key)

  /** Gets a Long array. */
  def getLongArray(key: String): Array[Long] = get(key)

  /** Gets a Double array. */
  def getDoubleArray(key: String): Array[Double] = get(key)

  /** Gets a Boolean array. */
  def getBooleanArray(key: String): Array[Boolean] = get(key)

  /** Gets a String array. */
  def getStringArray(key: String): Array[String] = get(key)

  /** Gets a Metadata array. */
  def getMetadataArray(key: String): Array[Metadata] = get(key)


  override def equals(obj: Any): Boolean = {
    obj match {
      case that: Metadata if map.size == that.map.size =>
        map.keysIterator.forall { key =>
          that.map.get(key) match {
            case Some(otherValue) =>
              val ourValue = map(key)
              (ourValue, otherValue) match {
                case (v0: Array[Long], v1: Array[Long]) => java.util.Arrays.equals(v0, v1)
                case (v0: Array[Double], v1: Array[Double]) => java.util.Arrays.equals(v0, v1)
                case (v0: Array[Boolean], v1: Array[Boolean]) => java.util.Arrays.equals(v0, v1)
                case (v0: Array[AnyRef], v1: Array[AnyRef]) => java.util.Arrays.equals(v0, v1)
                case (v0, v1) => v0 == v1
              }
            case None => false
          }
        }
      case other =>
        false
    }
  }

  private lazy val _hashCode: Int = Metadata.hash(this)
  override def hashCode: Int = _hashCode

  private def get[T](key: String): T = {
    map(key).asInstanceOf[T]
  }
}

object Metadata {

  private[this] val _empty = new Metadata(Map.empty)

  /** Returns an empty Metadata. */
  def empty: Metadata = _empty

  /** Computes the hash code for the types we support. */
  private def hash(obj: Any): Int = {
    obj match {
      case map: Map[_, _] =>
        map.mapValues(hash).##
      case arr: Array[_] =>
        // Seq.empty[T] has the same hashCode regardless of T.
        arr.toSeq.map(hash).##
      case x: Long =>
        x.##
      case x: Double =>
        x.##
      case x: Boolean =>
        x.##
      case x: String =>
        x.##
      case x: Metadata =>
        hash(x.map)
      case other =>
        throw new RuntimeException(s"Do not support type ${other.getClass}.")
    }
  }
}

/**
  *
  * Builder for [[Metadata]]. If there is a key collision, the latter will overwrite the former.
  */
class MetadataBuilder {

  private val map: mutable.Map[String, Any] = mutable.Map.empty

  /** Returns the immutable version of this map.  Used for java interop. */
  protected def getMap = map.toMap

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
