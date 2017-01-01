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

package io.cebes.persistence.cache

import com.google.common.cache._
import com.typesafe.scalalogging.LazyLogging
import io.cebes.persistence.KeyValuePersistence

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * Support a [[LoadingCache]] by a [[KeyValuePersistence]].
  * When the [[LoadingCache]] asks for a value, this will query the [[KeyValuePersistence]]
  * When an element is evicted from the [[LoadingCache]], it will get persisted into the [[KeyValuePersistence]]
  *
  * If [[removalFilter]] is specified, the evicted value will only be persisted
  * when removalFilter(key, value) returns true
  */
class CachePersistenceSupporter[K, V](val persistence: KeyValuePersistence[K, V],
                                      val removalFilter: Option[(K, V) => Boolean] = None)
  extends CacheLoader[K, V] with RemovalListener[K, V] with LazyLogging {

  @throws[NoSuchElementException]("If the key doesn't exist")
  override def load(key: K): V = {
    persistence.get(key) match {
      case Some(v) => v
      case None => throw new NoSuchElementException(s"Key ${key.toString} not found in the persistence storage")
    }
  }

  override def onRemoval(notification: RemovalNotification[K, V]): Unit = {
    if (removalFilter.isEmpty || removalFilter.get(notification.getKey, notification.getValue)) {
      Future(persistence.upsert(notification.getKey, notification.getValue))
    } else {
      logger.info(s"Item evicted from LoadingCache without being persisted: ${notification.toString}")
    }
  }

  /**
    * Returns a new [[CachePersistenceSupporter]] object with the given removalFilter
    */
  def withRemovalFilter(removalFilter: (K, V) => Boolean): CachePersistenceSupporter[K, V] =
    new CachePersistenceSupporter[K, V](persistence, Some(removalFilter))
}