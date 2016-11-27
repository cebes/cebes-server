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

import java.sql.{Connection, ResultSet, SQLSyntaxErrorException}

import com.typesafe.scalalogging.slf4j.LazyLogging
import io.cebes.persistence.KeyValuePersistence

import scala.util.{Failure, Success, Try}

case class JdbcPersistenceColumn(name: String, spec: String)

/**
  * Implementation of [[KeyValuePersistence]] using JDBC
  *
  * A table with the given name will be created.
  *
  * The type of the key in the SQL table will be always VARCHAR(200),
  * it is user's responsibility to make sure the `toString()` method of the
  * key type ([[K]]) produces meaningful values.
  */
abstract class JdbcPersistence[K <: Any, V](val url: String,
                                            val userName: String,
                                            val password: String,
                                            val suggestedTableName: String,
                                            val driver: String = "com.mysql.cj.jdbc.Driver"
                                           ) extends KeyValuePersistence[K, V] with LazyLogging {

  val tableName: String = withConnection { c =>
    JdbcPersistence.ensureTableExists(c, valueSchema, suggestedTableName)
  }

  /////////////////////////////////////////////////////////////////////////////
  // To be overridden
  /////////////////////////////////////////////////////////////////////////////

  /**
    * Convert the result set to an instance of the Value
    */
  protected def sqlToValue(result: ResultSet): V

  /**
    * Return a sequence of values that is used to insert the value into the JDBC table
    * The returned sequence must have the same length with [[valueSchema]].
    */
  protected def valueToSql(value: V): Seq[Any]

  /**
    * The schema for the values (excluding the key)
    */
  protected def valueSchema: Seq[JdbcPersistenceColumn]

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
    val stmt = c.prepareStatement(s"SELECT * FROM $tableName WHERE `${JdbcPersistence.keyColumnName}` = ?")
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
    val stmt = c.prepareStatement(s"DELETE FROM $tableName WHERE `${JdbcPersistence.keyColumnName}` = ?")
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

object JdbcPersistence extends LazyLogging {

  private val keyColumnName = "key"

  protected def ensureTableExists(connection: Connection,
                                  valueSchema: Seq[JdbcPersistenceColumn],
                                  suggestedTableName: String): String = {
    require(valueSchema.nonEmpty, "Empty value schema")

    var newTableName = suggestedTableName
    var incompatibleTable = true
    do {
      // query the table and check the schema, if it is similar to the requested schema
      val stmtSchema = connection.prepareStatement(s"SELECT * FROM $newTableName LIMIT 1")
      Try(stmtSchema.executeQuery().getMetaData) match {
        case Success(rsmd) =>
          incompatibleTable = rsmd.getColumnCount != valueSchema.length + 1 ||
            valueSchema.zipWithIndex.exists {
              case (col, idx) => !rsmd.getColumnName(idx + 1).equalsIgnoreCase(col.name)
            } ||
            !rsmd.getColumnName(rsmd.getColumnCount).equalsIgnoreCase(keyColumnName)

          if (incompatibleTable) {
            val newerTableName = s"${newTableName}_${System.currentTimeMillis / 1000}"
            logger.warn(s"The suggested table name $newTableName exists and seems to have " +
              s"different column names. Trying again with table named $newerTableName")
            newTableName = newerTableName
          }
        case Failure(_: SQLSyntaxErrorException) =>
          incompatibleTable = false
        case Failure(f) => throw f
      }
    } while (incompatibleTable)

    val stmt = connection.prepareStatement(s"CREATE TABLE IF NOT EXISTS $newTableName (" +
      valueSchema.map(col => s"`${col.name}` ${col.spec}").mkString(", ") +
      s", `$keyColumnName` VARCHAR(200) NOT NULL, PRIMARY KEY (`$keyColumnName`))")
    JdbcUtil.cleanJdbcCall(stmt)(_.close())(_.executeUpdate())
    newTableName
  }
}
