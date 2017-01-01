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

package io.cebes.df.store

import java.util.UUID

import io.cebes.df.Dataframe

/**
  * Storing dataframes, maybe backed by a LoadingCache
  * Subclasses of this trait should be singleton.
  */
trait DataframeStore {

  /**
    * Store the dataframe. If there is already a Dataframe with the same key,
    * it will be overwritten.
    * Return the newly added [[Dataframe]] (whatever passed in this function)
    */
  def add(dataframe: Dataframe): Dataframe

  /**
    * Get the Dataframe with the given ID, if any
    */
  def get(id: UUID): Option[Dataframe]

  /**
    * Persist the given dataframe.
    * The internals of this method depends on the backend,
    * so any assumption about how the dataframe is persisted
    * is only valid in the class that implements this trait.
    */
  def persist(dataframe: Dataframe): Unit

  /**
    * Unpersist the [[Dataframe]] of the given ID, if and only if:
    * 1) there is such a [[Dataframe]] in the store, and
    * 2) the [[Dataframe]] should not be persisted (i.e. it is not tagged, etc..)
    *
    * Return the (possibly unpersisted) [[Dataframe]] if it exists
    */
  def unpersist(dfId: UUID): Option[Dataframe]

  /**
    * Get the [[Dataframe]] with the given ID
    * Throws [[IllegalArgumentException]] if the ID doesn't exist in the store
    */
  def apply(id: UUID): Dataframe = get(id) match {
    case Some(df) => df
    case None =>
      throw new IllegalArgumentException(s"Dataframe ID not found: ${id.toString}")
  }
}
