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

import java.sql.ResultSet
import java.util.UUID

import io.cebes.persistence.helpers.TestPropertyHelper
import org.scalatest.FunSuite

case class Dummy(field1: Int, field2: Float)

case class DummyJdbcPersistence(override val url: String,
                                override val userName: String,
                                override val password: String,
                                override val suggestedTableName: String,
                                override val driver: String)
  extends JdbcPersistence[UUID, Dummy](url, userName, password, suggestedTableName, driver) {

  override protected def sqlToValue(result: ResultSet): Dummy = {
    Dummy(result.getInt("field1"), result.getFloat("field2"))
  }

  override protected def valueToSql(value: Dummy): Seq[Any] = Seq(value.field1, value.field2)

  override protected def valueSchema: Seq[JdbcPersistenceColumn] = Seq(
    JdbcPersistenceColumn("field1", "INT"), JdbcPersistenceColumn("field2", "FLOAT"))
}

case class Bar(f2: Float, f3: Int)

case class BarJdbcPersistence(override val url: String,
                                override val userName: String,
                                override val password: String,
                                override val suggestedTableName: String,
                                override val driver: String)
  extends JdbcPersistence[Int, Bar](url, userName, password, suggestedTableName, driver) {

  override protected def sqlToValue(result: ResultSet): Bar = {
    Bar(result.getFloat("f2"), result.getInt("f3"))
  }

  override protected def valueToSql(value: Bar): Seq[Any] = Seq(value.f2, value.f3)

  override protected def valueSchema: Seq[JdbcPersistenceColumn] = Seq(
    JdbcPersistenceColumn("f2", "FLOAT"), JdbcPersistenceColumn("f3", "INT"))
}

class JdbcPersistenceSuite extends FunSuite with TestPropertyHelper {

  test("persistence with UUID key", JdbcTestsEnabled) {
    val tableName = s"${this.getClass.getSimpleName.replace(".", "_")}_uuid_key"

    val persistence = DummyJdbcPersistence(properties.jdbcUrl, properties.jdbcUsername,
      properties.jdbcPassword, tableName, properties.jdbcDriver)

    val key = UUID.randomUUID()

    persistence.lookup(key) match {
      case Some(_) => persistence.remove(key)
      case None =>
    }
    assert(persistence.lookup(key).isEmpty)

    persistence.store(key, Dummy(100, 219.0f))
    val v = persistence.lookup(key)
    assert(v.nonEmpty)
    assert(v.get.field1 === 100)
    assert(v.get.field2 === 219.0f)

    // replace
    persistence.store(key, Dummy(-100, -219.0f))
    val v2 = persistence.lookup(key)
    assert(v2.nonEmpty)
    assert(v2.get.field1 === -100)
    assert(v2.get.field2 === -219.0f)

    persistence.remove(key)
    assert(persistence.lookup(key).isEmpty)

    // drop the table
    persistence.dropTable()
  }

  test("persistence with Int key", JdbcTestsEnabled) {
    val tableName = s"${this.getClass.getSimpleName.replace(".", "_")}_int_key"

    val persistence = BarJdbcPersistence(properties.jdbcUrl, properties.jdbcUsername,
      properties.jdbcPassword, tableName, properties.jdbcDriver)

    val key = 100

    persistence.lookup(key) match {
      case Some(_) => persistence.remove(key)
      case None =>
    }
    assert(persistence.lookup(key).isEmpty)

    persistence.store(key, Bar(20.0f, 219))
    val v = persistence.lookup(key)
    assert(v.nonEmpty)
    assert(v.get.f2 === 20.0f)
    assert(v.get.f3 === 219)

    // replace
    persistence.store(key, Bar(-100.0f, -219))
    val v2 = persistence.lookup(key)
    assert(v2.nonEmpty)
    assert(v2.get.f2 === -100.0f)
    assert(v2.get.f3 === -219)

    persistence.remove(key)
    assert(persistence.lookup(key).isEmpty)

    // drop the table
    persistence.dropTable()
  }

  test("persistence with incompatible table", JdbcTestsEnabled) {

    val tableName = s"${this.getClass.getSimpleName.replace(".", "_")}_incompatible"

    val persistence = DummyJdbcPersistence(properties.jdbcUrl, properties.jdbcUsername,
      properties.jdbcPassword, tableName, properties.jdbcDriver)
    assert(persistence.tableName === tableName)

    val key = UUID.randomUUID()

    persistence.lookup(key) match {
      case Some(_) => persistence.remove(key)
      case None =>
    }
    assert(persistence.lookup(key).isEmpty)

    persistence.store(key, Dummy(100, 219.0f))
    val v = persistence.lookup(key)
    assert(v.nonEmpty)
    assert(v.get.field1 === 100)
    assert(v.get.field2 === 219.0f)

    // replace
    persistence.store(key, Dummy(-100, -219.0f))
    val v2 = persistence.lookup(key)
    assert(v2.nonEmpty)
    assert(v2.get.field1 === -100)
    assert(v2.get.field2 === -219.0f)

    persistence.remove(key)
    assert(persistence.lookup(key).isEmpty)

    // using the same table again for a different persistence
    val persistenceBar = BarJdbcPersistence(properties.jdbcUrl, properties.jdbcUsername,
      properties.jdbcPassword, tableName, properties.jdbcDriver)
    assert(persistenceBar.tableName !== tableName)

    val keyBar = 100

    persistenceBar.lookup(keyBar) match {
      case Some(_) => persistenceBar.remove(keyBar)
      case None =>
    }
    assert(persistenceBar.lookup(keyBar).isEmpty)

    persistenceBar.store(keyBar, Bar(20.0f, 219))
    val v3 = persistenceBar.lookup(keyBar)
    assert(v3.nonEmpty)
    assert(v3.get.f2 === 20.0f)
    assert(v3.get.f3 === 219)

    // replace
    persistenceBar.store(keyBar, Bar(-100.0f, -219))
    val v4 = persistenceBar.lookup(keyBar)
    assert(v4.nonEmpty)
    assert(v4.get.f2 === -100.0f)
    assert(v4.get.f3 === -219)

    persistenceBar.remove(keyBar)
    assert(persistenceBar.lookup(keyBar).isEmpty)

    persistenceBar.dropTable()
    persistence.dropTable()
  }
}
