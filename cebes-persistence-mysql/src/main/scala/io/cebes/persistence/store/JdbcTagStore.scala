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

import io.cebes.common.HasId
import io.cebes.tag.Tag
import io.cebes.store.{TagEntry, TagStore}
import io.cebes.persistence.ClosableIterator
import io.cebes.persistence.jdbc.{JdbcPersistenceBuilder, JdbcPersistenceColumn}
import io.cebes.prop.types.MySqlBackendCredentials

import scala.collection.mutable


/**
  * An implementation of [[TagStore]] with JDBC persistence backend.
  */
abstract class JdbcTagStore[T <: HasId](mySqlCreds: MySqlBackendCredentials, tableName: String) extends TagStore[T] {

  private val jdbcStore = JdbcPersistenceBuilder.newBuilder[Tag, TagEntry]()
    .withCredentials(mySqlCreds.url, mySqlCreds.userName, mySqlCreds.password,
      tableName, mySqlCreds.driver)
    .withValueSchema(Seq(
      JdbcPersistenceColumn("created_at", "BIGINT"),
      JdbcPersistenceColumn("object_id", "VARCHAR(200)")
    ))
    .withValueToSeq(v => Seq(v.createdAt, v.objectId.toString))
    .withSqlToValue { case (_, v) => TagEntry(v.getLong(1), UUID.fromString(v.getString(2))) }
    .withStrToKey(Tag.fromString)
    .build()

  override def insert(tag: Tag, id: UUID): Unit = jdbcStore.insert(tag, TagEntry(System.currentTimeMillis(), id))

  override def remove(tag: Tag): Option[TagEntry] = jdbcStore.remove(tag)

  override def get(tag: Tag): Option[TagEntry] = jdbcStore.get(tag)

  override def find(id: UUID): Seq[Tag] = {
    val tags = jdbcStore.findValue(TagEntry(0, id), excludedFields = Seq("created_at"))
    val results = mutable.ListBuffer.empty[Tag]
    try {
      while (tags.hasNext) {
        results += tags.next()
      }
    } finally {
      tags.close()
    }
    results
  }

  override def elements: ClosableIterator[(Tag, TagEntry)] = jdbcStore.elements
}
