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
 * Created by phvu on 27/11/2016.
 */

package io.cebes.persistence.jdbc

import java.util.UUID

import io.cebes.persistence.helpers.TestPropertyHelper
import org.scalatest.FunSuite

case class Foo(field1: Int, field2: Float)

case class Bar(f2: Float, f3: Int)

class JdbcPersistenceSuite extends FunSuite with TestPropertyHelper {

  test("persistence with UUID key", JdbcTestsEnabled) {
    val tableName = s"${this.getClass.getSimpleName.replace(".", "_")}_uuid_key"

    val persistence = JdbcPersistenceBuilder.newBuilder[UUID, Foo]()
      .withCredentials(properties.jdbcUrl, properties.jdbcUsername,
        properties.jdbcPassword, tableName, properties.jdbcDriver)
      .withValueSchema(Seq(JdbcPersistenceColumn("field1", "INT"), JdbcPersistenceColumn("field2", "FLOAT")))
      .withValueToSeq(v => Seq(v.field1, v.field2))
      .withSqlToValue { case (_, s) => Foo(s.getInt("field1"), s.getFloat("field2")) }
      .build()

    val key = UUID.randomUUID()

    persistence.get(key) match {
      case Some(_) => persistence.remove(key)
      case None =>
    }
    assert(persistence.get(key).isEmpty)

    persistence.upsert(key, Foo(100, 219.0f))
    val v = persistence.get(key)
    assert(v.nonEmpty)
    assert(v.get.field1 === 100)
    assert(v.get.field2 === 219.0f)

    // replace
    persistence.upsert(key, Foo(-100, -219.0f))
    val v2 = persistence.get(key)
    assert(v2.nonEmpty)
    assert(v2.get.field1 === -100)
    assert(v2.get.field2 === -219.0f)

    persistence.remove(key)
    assert(persistence.get(key).isEmpty)

    // drop the table
    persistence.dropTable()
  }

  test("persistence with Int key", JdbcTestsEnabled) {
    val tableName = s"${this.getClass.getSimpleName.replace(".", "_")}_int_key"

    val persistence = JdbcPersistenceBuilder.newBuilder[Int, Bar]()
      .withCredentials(properties.jdbcUrl, properties.jdbcUsername,
        properties.jdbcPassword, tableName, properties.jdbcDriver)
      .withValueSchema(Seq(JdbcPersistenceColumn("f2", "FLOAT"), JdbcPersistenceColumn("f3", "INT")))
      .withValueToSeq(v => Seq(v.f2, v.f3))
      .withSqlToValue { case (_, s) => Bar(s.getFloat("f2"), s.getInt("f3")) }
      .build()

    val key = 100

    persistence.get(key) match {
      case Some(_) => persistence.remove(key)
      case None =>
    }
    assert(persistence.get(key).isEmpty)

    persistence.upsert(key, Bar(20.0f, 219))
    val v = persistence.get(key)
    assert(v.nonEmpty)
    assert(v.get.f2 === 20.0f)
    assert(v.get.f3 === 219)

    // replace
    persistence.upsert(key, Bar(-100.0f, -219))
    val v2 = persistence.get(key)
    assert(v2.nonEmpty)
    assert(v2.get.f2 === -100.0f)
    assert(v2.get.f3 === -219)

    persistence.remove(key)
    assert(persistence.get(key).isEmpty)

    // drop the table
    persistence.dropTable()
  }

  test("persistence with incompatible table", JdbcTestsEnabled) {

    val tableName = s"${this.getClass.getSimpleName.replace(".", "_")}_incompatible"

    val persistence = JdbcPersistenceBuilder.newBuilder[UUID, Foo]()
      .withCredentials(properties.jdbcUrl, properties.jdbcUsername,
        properties.jdbcPassword, tableName, properties.jdbcDriver)
      .withValueSchema(Seq(JdbcPersistenceColumn("field1", "INT"), JdbcPersistenceColumn("field2", "FLOAT")))
      .withValueToSeq(v => Seq(v.field1, v.field2))
      .withSqlToValue { case (_, s) => Foo(s.getInt("field1"), s.getFloat("field2")) }
      .build()
    assert(persistence.tableName === tableName)

    val key = UUID.randomUUID()

    persistence.get(key) match {
      case Some(_) => persistence.remove(key)
      case None =>
    }
    assert(persistence.get(key).isEmpty)

    persistence.upsert(key, Foo(100, 219.0f))
    val v = persistence.get(key)
    assert(v.nonEmpty)
    assert(v.get.field1 === 100)
    assert(v.get.field2 === 219.0f)

    // replace
    persistence.upsert(key, Foo(-100, -219.0f))
    val v2 = persistence.get(key)
    assert(v2.nonEmpty)
    assert(v2.get.field1 === -100)
    assert(v2.get.field2 === -219.0f)

    persistence.remove(key)
    assert(persistence.get(key).isEmpty)

    // using the same table again for a different persistence
    val persistenceBar = JdbcPersistenceBuilder.newBuilder[Int, Bar]()
      .withCredentials(properties.jdbcUrl, properties.jdbcUsername,
        properties.jdbcPassword, tableName, properties.jdbcDriver)
      .withValueSchema(Seq(JdbcPersistenceColumn("f2", "FLOAT"), JdbcPersistenceColumn("f3", "INT")))
      .withValueToSeq(v => Seq(v.f2, v.f3))
      .withSqlToValue { case (_, s) => Bar(s.getFloat("f2"), s.getInt("f3")) }
      .build()
    assert(persistenceBar.tableName !== tableName)

    val keyBar = 100

    persistenceBar.get(keyBar) match {
      case Some(_) => persistenceBar.remove(keyBar)
      case None =>
    }
    assert(persistenceBar.get(keyBar).isEmpty)

    persistenceBar.upsert(keyBar, Bar(20.0f, 219))
    val v3 = persistenceBar.get(keyBar)
    assert(v3.nonEmpty)
    assert(v3.get.f2 === 20.0f)
    assert(v3.get.f3 === 219)

    // replace
    persistenceBar.upsert(keyBar, Bar(-100.0f, -219))
    val v4 = persistenceBar.get(keyBar)
    assert(v4.nonEmpty)
    assert(v4.get.f2 === -100.0f)
    assert(v4.get.f3 === -219)

    persistenceBar.remove(keyBar)
    assert(persistenceBar.get(keyBar).isEmpty)

    persistenceBar.dropTable()
    persistence.dropTable()
  }
}
