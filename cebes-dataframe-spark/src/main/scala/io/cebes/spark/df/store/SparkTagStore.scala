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

package io.cebes.spark.df.store

import java.util.UUID

import io.cebes.common.Tag
import io.cebes.df.store.TagStore

class SparkTagStore extends TagStore {

  override def add(tag: Tag, id: UUID): Unit = ???

  override def remove(tag: Tag): Option[UUID] = ???

  override def elements: Iterator[(Tag, UUID)] = ???
}
