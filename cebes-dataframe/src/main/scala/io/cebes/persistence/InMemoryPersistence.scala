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
 * Created by phvu on 29/11/2016.
 */

package io.cebes.persistence

import scala.collection.mutable

/**
  * A simple implementation of [[KeyValuePersistence]], using a mutable HashMap
  */
class InMemoryPersistence[K, V](private val map: mutable.Map[K, V]) extends KeyValuePersistence[K, V] {

  def this() = this(mutable.HashMap.empty[K, V])

  def this(initials: Map[K, V]) = this(mutable.HashMap(initials.toSeq: _*))

  /**
    * Store the value associated with the key.
    * When the key is existed, its value will be updated
    */
  override def add(key: K, value: V): Unit = map.put(key, value)

  override def get(key: K): Option[V] = map.get(key)

  override def remove(key: K): Unit = map.remove(key)
}
