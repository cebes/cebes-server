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
 * Created by phvu on 15/12/2016.
 */

package io.cebes.store

import java.util.UUID

import io.cebes.common.HasId

/**
  * Storing objects, indexed by a UUID.
  * The store maybe backed by a LoadingCache.
  */
trait CachedStore[T <: HasId] {

  /**
    * Store the object. If there is already a object with the same key,
    * it will be overwritten.
    * Return the newly added object (whatever passed in this function)
    */
  def add(obj: T): T

  /**
    * Get the object with the given ID, if any
    */
  def get(id: UUID): Option[T]

  /**
    * Persist the given object.
    * The internals of this method depends on the backend,
    * so any assumption about how the object is persisted
    * is only valid in the class that implements this trait.
    */
  def persist(obj: T): Unit

  /**
    * Unpersist the object of the given ID, if and only if:
    * 1) there is such a object in the store, and
    * 2) the object should not be persisted (i.e. it is not tagged, etc..)
    *
    * Return the (possibly unpersisted) object if it exists
    */
  def unpersist(id: UUID): Option[T]

  /**
    * Get the object with the given ID
    * Throws [[IllegalArgumentException]] if the ID doesn't exist in the store
    */
  def apply(id: UUID): T = get(id) match {
    case Some(obj) => obj
    case None =>
      throw new IllegalArgumentException(s"Object ID not found: ${id.toString}")
  }
}
