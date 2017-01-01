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
 * Created by phvu on 26/11/2016.
 */

package io.cebes.persistence.jdbc

import java.sql._

import com.typesafe.scalalogging.LazyLogging
import io.cebes.persistence.{ClosableIterator, KeyValuePersistence}
import org.apache.commons.dbcp2.BasicDataSource

/**
  * Implementation of [[KeyValuePersistence]] using JDBC
  *
  * A table with the given name will be created.
  *
  * The type of the key in the SQL table will be always VARCHAR(200),
  * it is user's responsibility to make sure the `toString()` method of the
  * key type ([[K]]) produces meaningful values.
  *
  * See [[JdbcPersistenceBuilder]] for examples on how to use this.
  */
class JdbcPersistence[K <: Any, V] private[jdbc](val url: String,
                                                 val userName: String,
                                                 val password: String,
                                                 val tableName: String,
                                                 val driver: String = "com.mysql.cj.jdbc.Driver",
                                                 val keyColumnName: String = "key",
                                                 val valueSchema: Seq[JdbcPersistenceColumn],
                                                 val valueToSql: V => Seq[Any],
                                                 val sqlToValue: (K, ResultSet) => V,
                                                 val strToKey: String => K
                                                ) extends KeyValuePersistence[K, V] with LazyLogging {

  private lazy val dataSource = {
    val ds = new BasicDataSource()
    ds.setDriverClassName(driver)
    ds.setUrl(url)
    ds.setUsername(userName)
    ds.setPassword(password)
    ds.setInitialSize(1)
    ds
  }

  /////////////////////////////////////////////////////////////////////////////
  // KeyValuePersistence APIs
  /////////////////////////////////////////////////////////////////////////////

  override def insert(key: K, value: V): Unit = withConnection { c =>
    val valuePlaceHolder = valueSchema.map(_ => "?").mkString(", ")
    val stmt = c.prepareStatement(s"INSERT INTO $tableName VALUES ($valuePlaceHolder, ?) ")
    val values = safeValueSeq(value)

    values.zipWithIndex.foreach {
      case (v, idx) =>
        stmt.setObject(idx + 1, v)
    }
    stmt.setString(values.length + 1, key.toString)
    try {
      JdbcUtil.cleanJdbcCall(stmt)(_.close())(_.executeUpdate())
    } catch {
      case _: SQLIntegrityConstraintViolationException =>
        throw new IllegalArgumentException(s"Duplicated key: ${key.toString}")
    }
  }

  override def upsert(key: K, value: V): Unit = withConnection { c =>
    val valuePlaceHolder = valueSchema.map(_ => "?").mkString(", ")
    val updatePlaceHolder = valueSchema.map(col => s"`${col.name}` = ?").mkString(", ")

    val stmt = c.prepareStatement(s"INSERT INTO $tableName VALUES ($valuePlaceHolder, ?) " +
      s"ON DUPLICATE KEY UPDATE $updatePlaceHolder")
    val values = safeValueSeq(value)

    values.zipWithIndex.foreach {
      case (v, idx) =>
        stmt.setObject(idx + 1, v)
        stmt.setObject(idx + 2 + values.length, v)
    }
    stmt.setString(values.length + 1, key.toString)
    JdbcUtil.cleanJdbcCall(stmt)(_.close())(_.executeUpdate())
  }

  override def get(key: K): Option[V] = withConnection { c =>
    val stmt = c.prepareStatement(s"SELECT * FROM $tableName WHERE `$keyColumnName` = ?")
    stmt.setString(1, key.toString)

    JdbcUtil.cleanJdbcCall(stmt)(_.close()) { s =>
      JdbcUtil.cleanJdbcCall(s.executeQuery())(_.close()) { result =>
        if (result.next()) {
          Some(sqlToValue(key, result))
        } else {
          None
        }
      }
    }
  }

  override def remove(key: K): Option[V] = withConnection { c =>
    // race condition might happen
    val value = get(key)
    if (value.isDefined) {
      val stmt = c.prepareStatement(s"DELETE FROM $tableName WHERE `$keyColumnName` = ?")
      stmt.setString(1, key.toString)
      JdbcUtil.cleanJdbcCall(stmt)(_.close()) { s =>
        val result = s.executeUpdate()
        if (result != 1) {
          logger.warn(s"Deleted $result rows from JDBC persistence, in table $tableName")
        }
      }
    }
    value
  }

  override def elements: ClosableIterator[(K, V)] = {
    val connection = dataSource.getConnection()
    val stmt = connection.prepareStatement(s"SELECT * FROM $tableName")
    new ResultSetIterable(connection, stmt, result => {
      val key = strToKey(result.getString(valueSchema.length + 1))
      (key, sqlToValue(key, result))
    })
  }

  override def findValue(value: V): ClosableIterator[K] = {
    val connection = dataSource.getConnection()
    val valuePlaceHolder = valueSchema.map(s => s"`${s.name}` = ?").mkString(" AND ")
    val stmt = connection.prepareStatement(s"SELECT * FROM $tableName WHERE $valuePlaceHolder")
    val values = safeValueSeq(value)
    values.zipWithIndex.foreach {
      case (v, idx) =>
        stmt.setObject(idx + 1, v)
    }
    new ResultSetIterable(connection, stmt, result => {
      strToKey(result.getString(valueSchema.length + 1))
    })
  }

  /////////////////////////////////////////////////////////////////////////////
  // private helpers
  /////////////////////////////////////////////////////////////////////////////

  private def safeValueSeq(value: V) = {
    val values = valueToSql(value)
    require(values.length == valueSchema.length,
      s"Invalid sequence of values. " +
        s"Expected a sequence of ${valueSchema.length} elements, got ${values.length} elements")
    values
  }

  private def withConnection[T](action: Connection => T) = {
    JdbcUtil.cleanJdbcCall(dataSource.getConnection)(_.close())(action)
  }
}
