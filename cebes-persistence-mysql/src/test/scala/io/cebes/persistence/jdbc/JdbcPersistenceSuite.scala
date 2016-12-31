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
import org.scalatest.{BeforeAndAfterAll, FunSuite}

case class Foo(field1: Int, field2: Float)

case class Bar(f2: Float, f3: Int)

class JdbcPersistenceSuite extends FunSuite with TestPropertyHelper with BeforeAndAfterAll {

  private val tableNameUUIDKey = s"${this.getClass.getSimpleName.replace(".", "_")}_uuid_key"
  private val tableNameUUIDKeyElements = s"${this.getClass.getSimpleName.replace(".", "_")}_uuid_key_find_elements"
  private val tableNameIntKey = s"${this.getClass.getSimpleName.replace(".", "_")}_int_key"
  private val tableNameIncompatible = s"${this.getClass.getSimpleName.replace(".", "_")}_incompatible"

  override def beforeAll(): Unit = {
    if (properties.hasJdbcCredentials) {
      dropTable(tableNameUUIDKey)
      dropTable(tableNameUUIDKeyElements)
      dropTable(tableNameIntKey)
      dropTable(tableNameIncompatible)
    }
  }

  private def dropTable(name: String) = {
    val connection = JdbcUtil.getConnection(properties.url, properties.userName,
      properties.password, properties.driver)
    JdbcUtil.cleanJdbcCall(connection)(_.close()) { c =>
      val stmt = c.prepareStatement(s"DROP TABLE IF EXISTS $name")
      JdbcUtil.cleanJdbcCall(stmt)(_.close())(_.executeUpdate())
    }
  }

  test("persistence with UUID key", JdbcTestsEnabled) {
    val persistence = JdbcPersistenceBuilder.newBuilder[UUID, Foo]()
      .withCredentials(properties.url, properties.userName,
        properties.password, tableNameUUIDKey, properties.driver)
      .withValueSchema(Seq(JdbcPersistenceColumn("field1", "INT"), JdbcPersistenceColumn("field2", "FLOAT")))
      .withValueToSeq(v => Seq(v.field1, v.field2))
      .withSqlToValue { case (_, s) => Foo(s.getInt("field1"), s.getFloat("field2")) }
      .withStrToKey(UUID.fromString)
      .build()

    val key = UUID.randomUUID()

    persistence.get(key) match {
      case Some(_) => persistence.remove(key)
      case None =>
    }
    assert(persistence.get(key).isEmpty)

    persistence.insert(key, Foo(300, 219.0f))
    val v0 = persistence.get(key)
    assert(v0.nonEmpty)
    assert(v0.get.field1 === 300)
    assert(v0.get.field2 === 219.0f)

    // insert the same key
    val ex = intercept[IllegalArgumentException] {
      persistence.insert(key, Foo(100, 219.0f))
    }
    assert(ex.getMessage.startsWith("Duplicated key"))

    // find values
    val keys1 = persistence.findValue(Foo(300, 219.0f))
    val seq1 = keys1.toSeq
    assert(seq1.length === 1)
    assert(seq1.head === key)
    keys1.close()

    val keys2 = persistence.findValue(Foo(100, 219.1f))
    assert(keys2.isEmpty)
    keys2.close()

    // elements
    val elements = persistence.elements
    assert(elements.size === 1)
    elements.close()

    // replace
    persistence.upsert(key, Foo(-100, -219.0f))
    val v2 = persistence.get(key)
    assert(v2.nonEmpty)
    assert(v2.get.field1 === -100)
    assert(v2.get.field2 === -219.0f)

    persistence.remove(key)
    assert(persistence.get(key).isEmpty)
  }

  test("persistence with UUID key - find and elements", JdbcTestsEnabled) {

    val persistence = JdbcPersistenceBuilder.newBuilder[UUID, Foo]()
      .withCredentials(properties.url, properties.userName,
        properties.password, tableNameUUIDKeyElements, properties.driver)
      .withValueSchema(Seq(JdbcPersistenceColumn("field1", "INT"), JdbcPersistenceColumn("field2", "FLOAT")))
      .withValueToSeq(v => Seq(v.field1, v.field2))
      .withSqlToValue { case (_, s) => Foo(s.getInt("field1"), s.getFloat("field2")) }
      .withStrToKey(UUID.fromString)
      .build()

    val keys = UUID.randomUUID() :: UUID.randomUUID() :: UUID.randomUUID() :: UUID.randomUUID() :: Nil
    assert(keys.length === 4)
    persistence.insert(keys.head, Foo(100, 219.0f))
    persistence.insert(keys(1), Foo(200, 219.0f))
    persistence.insert(keys(2), Foo(300, 219.0f))
    persistence.insert(keys(3), Foo(200, 219.0f))

    // find values
    val keys1 = persistence.findValue(Foo(200, 219.0f))
    val seq1 = keys1.toSeq
    assert(seq1.length === 2)
    assert(seq1.contains(keys(1)))
    assert(seq1.contains(keys(3)))
    keys1.close()

    // elements
    val elements = persistence.elements
    assert(elements.size === 4)
    elements.close()
  }

  test("persistence with Int key", JdbcTestsEnabled) {
    val persistence = JdbcPersistenceBuilder.newBuilder[Int, Bar]()
      .withCredentials(properties.url, properties.userName,
        properties.password, tableNameIntKey, properties.driver)
      .withValueSchema(Seq(JdbcPersistenceColumn("f2", "FLOAT"), JdbcPersistenceColumn("f3", "INT")))
      .withValueToSeq(v => Seq(v.f2, v.f3))
      .withSqlToValue { case (_, s) => Bar(s.getFloat("f2"), s.getInt("f3")) }
      .withStrToKey(_.toInt)
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
  }

  test("persistence with incompatible table", JdbcTestsEnabled) {

    val persistence = JdbcPersistenceBuilder.newBuilder[UUID, Foo]()
      .withCredentials(properties.url, properties.userName,
        properties.password, tableNameIncompatible, properties.driver)
      .withValueSchema(Seq(JdbcPersistenceColumn("field1", "INT"), JdbcPersistenceColumn("field2", "FLOAT")))
      .withValueToSeq(v => Seq(v.field1, v.field2))
      .withSqlToValue { case (_, s) => Foo(s.getInt("field1"), s.getFloat("field2")) }
      .withStrToKey(UUID.fromString)
      .build()
    assert(persistence.tableName === tableNameIncompatible)

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
      .withCredentials(properties.url, properties.userName,
        properties.password, tableNameIncompatible, properties.driver)
      .withValueSchema(Seq(JdbcPersistenceColumn("f2", "FLOAT"), JdbcPersistenceColumn("f3", "INT")))
      .withValueToSeq(v => Seq(v.f2, v.f3))
      .withSqlToValue { case (_, s) => Bar(s.getFloat("f2"), s.getInt("f3")) }
      .withStrToKey(_.toInt)
      .build()
    assert(persistenceBar.tableName !== tableNameIncompatible)

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

    dropTable(persistenceBar.tableName)
  }
}
