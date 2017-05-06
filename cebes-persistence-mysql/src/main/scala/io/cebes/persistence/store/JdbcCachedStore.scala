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
 */
package io.cebes.persistence.store

import java.util.UUID

import com.google.common.cache.{CacheBuilder, LoadingCache}
import com.google.common.util.concurrent.UncheckedExecutionException
import com.typesafe.scalalogging.LazyLogging
import io.cebes.common.HasId
import io.cebes.store.CachedStore
import io.cebes.persistence.cache.CachePersistenceSupporter
import io.cebes.persistence.jdbc.JdbcPersistence

/**
  * a [[CachedStore]] backed by a [[LoadingCache]],
  * which in turn backed by a [[JdbcPersistence]]
  */
abstract class JdbcCachedStore[T <: HasId](cacheSpec: String) extends CachedStore[T] with LazyLogging {

  private lazy val cache: LoadingCache[UUID, T] = {
    val supporter = new CachePersistenceSupporter[UUID, T](jdbcPersistence)
      .withRemovalFilter { (id, _) => shouldPersist(id) }
    CacheBuilder.from(cacheSpec).removalListener(supporter).build[UUID, T](supporter)
  }

  /**
    * The JDBC persistence that backs the LoadingCache.
    * To be defined by the subclasses
    */
  protected val jdbcPersistence: JdbcPersistence[UUID, T]

  /**
    * Check whether the object with the given ID should be persisted or not.
    * Subclass should override this
    */
  protected def shouldPersist(id: UUID): Boolean

  /** Custom logic that persist the object, if needed by the subclass */
  protected def doPersist(obj: T): Unit

  /** Custom logic to unpersist the object, if needed by the subclass */
  protected def doUnpersist(obj: T): Unit

  /////////////////////////////////////////////////////////////////////////////
  // override
  /////////////////////////////////////////////////////////////////////////////

  /**
    * Store the object.
    * If there is already a object with the same key, it will be overwritten.
    */
  override def add(obj: T): T = {
    cache.put(obj.id, obj)
    obj
  }

  /**
    * Get the object with the given ID, if any
    */
  override def get(id: UUID): Option[T] = {
    try {
      Some(cache.get(id))
    } catch {
      case e@(_: UncheckedExecutionException | _: IllegalArgumentException) =>
        logger.warn(s"Failed to get Dataframe for ID $id: ${e.getMessage}")
        None
    }
  }

  /**
    * Persist the given object.
    * The internals of this method depends on the backend,
    * so any assumption about how the object is persisted
    * is only valid in the class that implements this trait.
    */
  override def persist(obj: T): Unit = {
    jdbcPersistence.upsert(obj.id, obj)
    doPersist(obj)
  }

  /**
    * Unpersist the object of the given ID, if and only if:
    * 1) there is such a object in the store, and
    * 2) the object should not be persisted (i.e. it is not tagged, etc..)
    *
    * Return the (possibly unpersisted) object if it exists
    */
  override def unpersist(id: UUID): Option[T] = {
    val optionDf = get(id)
    if (optionDf.nonEmpty && !shouldPersist(id)) {
      // delete from the jdbc persistence
      jdbcPersistence.remove(id)
      doUnpersist(optionDf.get)
    }
    optionDf
  }
}
