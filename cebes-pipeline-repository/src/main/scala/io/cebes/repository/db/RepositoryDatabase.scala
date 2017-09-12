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
package io.cebes.repository.db

import java.sql.Timestamp

import org.squeryl.{Schema, Table}

case class Repository(id: Long,
                      name: String,
                      owner: String,
                      isPrivate: Boolean,
                      pullCount: Long)

case class RepositoryTag(id: Long,
                         repositoryId: Long,
                         name: String,
                         lastUpdate: Timestamp)

object RepositoryDatabase extends Schema {

  val repositories: Table[_] = table[Repository]
  val repositoryTags: Table[_] = table[RepositoryTag]
}
