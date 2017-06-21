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
 * Created by phvu on 26/11/2016.
 */

package io.cebes.persistence

/**
  * A general trait for key-value persistent storage
  */
trait KeyValuePersistence[K, V] {

  /**
    * Insert the element if the key doesn't exist.
    * If the key already exists, raise an exception.
    */
  def insert(key: K, value: V): Unit

  /**
    * Store the value associated with the key.
    * When the key is existed, its value will be updated
    */
  def upsert(key: K, value: V): Unit

  def get(key: K): Option[V]

  def remove(key: K): Option[V]

  /**
    * Return a [[ClosableIterator]] of the pairs (key, value)
    */
  def elements: ClosableIterator[(K, V)]

  /**
    * Find the entry with the given value,
    * may returns several keys.
    */
  def findValue(value: V, excludedFields: Seq[String] = Seq()): ClosableIterator[K]
}
