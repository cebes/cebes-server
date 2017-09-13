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

import com.typesafe.scalalogging.LazyLogging
import io.cebes.repository.db.SquerylEntrypoint._
import org.squeryl.{KeyedEntity, Schema, SquerylSQLException, Table}

case class Repository(id: Long,
                      name: String,
                      owner: String,
                      isPrivate: Boolean,
                      pullCount: Long)

case class RepositoryTag(id: Long,
                         repositoryId: Long,
                         name: String,
                         lastUpdate: Timestamp) extends KeyedEntity[Long]

object RepositoryDatabase extends Schema with LazyLogging {

  val repositories: Table[Repository] = table[Repository]
  val repositoryTags: Table[RepositoryTag] = table[RepositoryTag]

  on(repositories)(r => declare(
    r.name is(indexed, unique),
    r.pullCount defaultsTo 0L
  ))

  on(repositoryTags)(t => declare(
    t.name is(indexed, unique)
  ))

  def initialize(): Unit = {
    // this is rather bad. Should generate the DDL separately and evolve it manually
    transaction {
      try {
        from(repositories)(r => select(r)).size
      } catch {
        case ex: SquerylSQLException =>
          logger.error("Failed to select some entries from the DB. Trying to create the tables", ex)
          this.create
        case ex =>
          throw ex
      }
    }
  }
}
