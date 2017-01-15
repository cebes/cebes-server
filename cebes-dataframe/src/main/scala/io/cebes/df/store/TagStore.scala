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
 * Created by phvu on 29/12/2016.
 */

package io.cebes.df.store

import java.util.UUID

import io.cebes.common.Tag
import io.cebes.persistence.ClosableIterator

trait TagStore {

  /**
    * add a new tag for the given ID
    * Throws exception if the tag exists
    */
  def insert(tag: Tag, id: UUID): Unit

  /**
    * Remove the given tag
    */
  def remove(tag: Tag): Option[UUID]

  /**
    * Get the ID with the given tag
    */
  def get(tag: Tag): Option[UUID]

  /**
    * Find the given UUID in this store, return a sequence of tags
    */
  def find(id: UUID): Seq[Tag]

  /**
    * Get all tags
    */
  def elements: ClosableIterator[(Tag, UUID)]
}

trait DataframeTagStore extends TagStore

trait PipelineTagStore extends TagStore
