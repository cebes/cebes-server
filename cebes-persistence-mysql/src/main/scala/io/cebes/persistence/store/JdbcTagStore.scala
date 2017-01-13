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

import io.cebes.common.Tag
import io.cebes.df.store.TagStore
import io.cebes.persistence.ClosableIterator
import io.cebes.persistence.jdbc.{JdbcPersistenceBuilder, JdbcPersistenceColumn}
import io.cebes.prop.types.MySqlBackendCredentials

import scala.collection.mutable


/**
  * An implementation of [[io.cebes.df.store.TagStore]] with JDBC persistence backend.
  */
abstract class JdbcTagStore(mySqlCreds: MySqlBackendCredentials, tableName: String) extends TagStore {

  private val jdbcStore = JdbcPersistenceBuilder.newBuilder[Tag, UUID]()
    .withCredentials(mySqlCreds.url, mySqlCreds.userName, mySqlCreds.password,
      tableName, mySqlCreds.driver)
    .withValueSchema(Seq(
      JdbcPersistenceColumn("uuid", "VARCHAR(200)")
    ))
    .withValueToSeq(v => Seq(v.toString))
    .withSqlToValue { case (_, v) => UUID.fromString(v.getString(1)) }
    .withStrToKey(Tag.fromString)
    .build()

  override def insert(tag: Tag, id: UUID): Unit = jdbcStore.insert(tag, id)

  override def remove(tag: Tag): Option[UUID] = jdbcStore.remove(tag)

  override def get(tag: Tag): Option[UUID] = jdbcStore.get(tag)

  override def find(id: UUID): Seq[Tag] = {
    val tags = jdbcStore.findValue(id)
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

  override def elements: ClosableIterator[(Tag, UUID)] = jdbcStore.elements
}
