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

import java.sql.{Connection, ResultSet}

import com.typesafe.scalalogging.slf4j.LazyLogging
import io.cebes.persistence.KeyValuePersistence

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
                                                 val sqlToValue: ResultSet => V
                                                ) extends KeyValuePersistence[K, V] with LazyLogging {

  /////////////////////////////////////////////////////////////////////////////
  // KeyValuePersistence APIs
  /////////////////////////////////////////////////////////////////////////////

  override def store(key: K, value: V): Unit = withConnection { c =>
    val valuePlaceHolder = valueSchema.map(_ => "?").mkString(", ")
    val updatePlaceHolder = valueSchema.map(col => s"`${col.name}` = ?").mkString(", ")

    val stmt = c.prepareStatement(s"INSERT INTO $tableName VALUES ($valuePlaceHolder, ?) " +
      s"ON DUPLICATE KEY UPDATE $updatePlaceHolder")

    val values = valueToSql(value)
    require(values.length == valueSchema.length,
      s"Invalid sequence of values. " +
        s"Expected a sequence of ${valueSchema.length} elements, got ${values.length} elements")

    values.zipWithIndex.foreach {
      case (v, idx) =>
        stmt.setObject(idx + 1, v)
        stmt.setObject(idx + 2 + values.length, v)
    }
    stmt.setString(values.length + 1, key.toString)

    JdbcUtil.cleanJdbcCall(stmt)(_.close())(_.executeUpdate())
  }

  override def lookup(key: K): Option[V] = withConnection { c =>
    val stmt = c.prepareStatement(s"SELECT * FROM $tableName WHERE `$keyColumnName` = ?")
    stmt.setString(1, key.toString)

    JdbcUtil.cleanJdbcCall(stmt)(_.close()) { s =>
      JdbcUtil.cleanJdbcCall(s.executeQuery())(_.close()) { result =>
        if (result.next()) {
          Some(sqlToValue(result))
        } else {
          None
        }
      }
    }
  }

  override def remove(key: K): Unit = withConnection { c =>
    val stmt = c.prepareStatement(s"DELETE FROM $tableName WHERE `$keyColumnName` = ?")
    stmt.setString(1, key.toString)
    JdbcUtil.cleanJdbcCall(stmt)(_.close()) { s =>
      val result = s.executeUpdate()
      if (result != 1) {
        logger.warn(s"Deleted $result rows from JDBC persistence, in table $tableName")
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  // private helpers
  /////////////////////////////////////////////////////////////////////////////

  private def withConnection[T](action: Connection => T) = {
    val connection = JdbcUtil.getConnection(url, userName, password, driver)
    JdbcUtil.cleanJdbcCall(connection)(_.close())(action)
  }

  /**
    * Drop the table that backs this persistence
    * For testing purpose only
    */
  private[jdbc] def dropTable(): Unit = withConnection { c =>
    val stmt = c.prepareStatement(s"DROP TABLE IF EXISTS $tableName")
    JdbcUtil.cleanJdbcCall(stmt)(_.close())(_.executeUpdate())
  }
}
