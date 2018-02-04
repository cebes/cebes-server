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
import io.cebes.persistence.jdbc.TableNames
import io.cebes.repository.db.SquerylEntrypoint._
import org.squeryl._
import org.squeryl.dsl.{ManyToOne, OneToMany}

case class Repository(id: Long,
                      name: String,
                      owner: String,
                      isPrivate: Boolean,
                      pullCount: Long) extends KeyedEntity[Long] {
  lazy val tags: OneToMany[RepositoryTag] = RepositoryDatabase.repoToTags.left(this)
}

case class RepositoryTag(id: Long,
                         repositoryId: Long,
                         name: String,
                         lastUpdate: Timestamp) extends KeyedEntity[Long] {
  lazy val repository: ManyToOne[Repository] = RepositoryDatabase.repoToTags.right(this)
}

object RepositoryDatabase extends Schema with LazyLogging {

  val repositories: Table[Repository] = table[Repository](TableNames.REPO_REPOSITORY)
  val repositoryTags: Table[RepositoryTag] = table[RepositoryTag](TableNames.REPO_REPOSITORY_TAG)

  on(repositories)(r => declare(
    r.name is(indexed, unique),
    r.pullCount defaultsTo 0L
  ))

  on(repositoryTags)(t => declare(
    columns(t.name, t.repositoryId) are(indexed, unique),
    t.name is indexed
  ))

  val repoToTags = oneToManyRelation(repositories, repositoryTags).via((r, t) => r.id === t.repositoryId)

  // the default constraint for all foreign keys in this schema :
  override def applyDefaultForeignKeyPolicy(foreignKeyDeclaration: ForeignKeyDeclaration) =
    foreignKeyDeclaration.constrainReference

  //now we will redefine some of the foreign key constraints :
  //if we delete a repo, we want all tags to be deleted
  repoToTags.foreignKeyDeclaration.constrainReference(onDelete.cascade)

  ////////////////////////////////////////////////////////////////////////////////////////////////////

  def initialize(): Unit = {
    // this is rather bad. Should generate the DDL separately and evolve it manually
    def tryQuery(created: Boolean): Unit = {
      try {
        transaction {
          from(repositories)(r => select(r)).size
        }
      } catch {
        case ex: SquerylSQLException =>
          if (created) {
            throw ex
          } else {
            logger.info("Failed to select some entries from the DB. Will try to create tables")
            logger.info(s"Error was: ${ex.getMessage}")
            transaction(this.create)
            tryQuery(true)
            logger.info("Created tables successfully")
          }
      }
    }
    tryQuery(false)
  }
}
