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
 * Created by phvu on 29/11/2016.
 */

package io.cebes.persistence.jdbc

import java.sql.{Connection, ResultSet, SQLSyntaxErrorException}

import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Success, Try}

/**
  * Builder of a [[JdbcPersistence]]
  * {{{
  *   case class Foo(field1: Int, field2: Float)
  *
  *   val tableName = "my_sql_table_name"
  *
  *   val persistence = JdbcPersistenceBuilder.newBuilder[UUID, Foo]()
  *     .withCredentials(jdbcUrl, jdbcUsername, jdbcPassword, tableName, jdbcDriver)
  *     .withValueSchema(Seq(JdbcPersistenceColumn("field1", "INT"), JdbcPersistenceColumn("field2", "FLOAT")))
  *     .withValueToSeq(v => Seq(v.field1, v.field2))
  *     .withSqlToValue(s => Foo(s.getInt("field1"), s.getFloat("field2")))
  *     .build()
  * }}}
  */
case class JdbcPersistenceBuilder[K, V] private(url: String,
                                                userName: String,
                                                password: String,
                                                suggestedTableName: String,
                                                driver: String = "com.mysql.cj.jdbc.Driver",
                                                valueSchema: Seq[JdbcPersistenceColumn] = Nil,
                                                sqlToValue: Option[ResultSet => V] = None,
                                                valueToSeq: Option[V => Seq[Any]] = None
                                               ) {

  private def this() = this("", "", "", "")

  def withUrl(jdbcUrl: String): JdbcPersistenceBuilder[K, V] =
    JdbcPersistenceBuilder[K, V](jdbcUrl, userName, password, suggestedTableName,
      driver, valueSchema, sqlToValue, valueToSeq)

  def withUserName(jdbcUserName: String): JdbcPersistenceBuilder[K, V] =
    JdbcPersistenceBuilder[K, V](url, jdbcUserName, password, suggestedTableName,
      driver, valueSchema, sqlToValue, valueToSeq)

  def withPassword(jdbcPassword: String): JdbcPersistenceBuilder[K, V] =
    JdbcPersistenceBuilder[K, V](url, userName, jdbcPassword, suggestedTableName,
      driver, valueSchema, sqlToValue, valueToSeq)

  def withTableName(tableName: String): JdbcPersistenceBuilder[K, V] =
    JdbcPersistenceBuilder[K, V](url, userName, password, tableName,
      driver, valueSchema, sqlToValue, valueToSeq)

  def withDriver(jdbcDriver: String): JdbcPersistenceBuilder[K, V] =
    JdbcPersistenceBuilder[K, V](url, userName, password, suggestedTableName,
      jdbcDriver, valueSchema, sqlToValue, valueToSeq)

  def withCredentials(jdbcUrl: String, jdbcUserName: String, jdbcPassword: String, tableName: String,
                      jdbcDriver: String): JdbcPersistenceBuilder[K, V] =
    withUrl(jdbcUrl).withUserName(jdbcUserName).withPassword(jdbcPassword).
      withTableName(tableName).withDriver(jdbcDriver)

  def withValueSchema(schema: Seq[JdbcPersistenceColumn]): JdbcPersistenceBuilder[K, V] =
    JdbcPersistenceBuilder[K, V](url, userName, password, suggestedTableName,
      driver, schema, sqlToValue, valueToSeq)

  def withSqlToValue(f: ResultSet => V): JdbcPersistenceBuilder[K, V] =
    JdbcPersistenceBuilder[K, V](url, userName, password, suggestedTableName,
      driver, valueSchema, Some(f), valueToSeq)

  def withValueToSeq(f: V => Seq[Any]): JdbcPersistenceBuilder[K, V] =
    JdbcPersistenceBuilder[K, V](url, userName, password, suggestedTableName,
      driver, valueSchema, sqlToValue, Some(f))

  def build(): JdbcPersistence[K, V] = {
    require(valueToSeq.nonEmpty && sqlToValue.nonEmpty, "Empty valueToSeq or sqlToValue functions")

    val tableName = withConnection(c => JdbcPersistenceBuilder.ensureTableExists(c, valueSchema, suggestedTableName))
    new JdbcPersistence[K, V](url, userName, password, tableName,
      driver, JdbcPersistenceBuilder.keyColumnName, valueSchema, valueToSeq.get, sqlToValue.get)
  }

  /////////////////////////////////////////////////////////////////////////////
  // private helpers
  /////////////////////////////////////////////////////////////////////////////

  private def withConnection[T](action: Connection => T) = {
    val connection = JdbcUtil.getConnection(url, userName, password, driver)
    JdbcUtil.cleanJdbcCall(connection)(_.close())(action)
  }
}

object JdbcPersistenceBuilder extends LazyLogging {

  private val keyColumnName = "key"

  def newBuilder[K, V](): JdbcPersistenceBuilder[K, V] = new JdbcPersistenceBuilder[K, V]()

  protected def ensureTableExists(connection: Connection,
                                  valueSchema: Seq[JdbcPersistenceColumn],
                                  suggestedTableName: String): String = {
    require(valueSchema.nonEmpty, "Empty value schema")
    require(!valueSchema.map(_.name).exists(_.equalsIgnoreCase(JdbcPersistenceBuilder.keyColumnName)),
      s"The given valueSchema has a column named ${JdbcPersistenceBuilder.keyColumnName}, " +
        s"which is reserved for the index key of this table. Please use a different name for that column.")

    var newTableName = suggestedTableName
    var incompatibleTable = true
    var incompatibleTableIdx = 0
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
            val newerTableName = f"${suggestedTableName}_$incompatibleTableIdx%05d"
            logger.warn(s"The suggested table name $newTableName exists and seems to have " +
              s"different column names. Trying again with table named $newerTableName")
            newTableName = newerTableName
            incompatibleTableIdx += 1
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
