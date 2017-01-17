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
case class JdbcPersistenceBuilder[K, V] private(url: String = "",
                                                userName: String = "",
                                                password: String = "",
                                                suggestedTableName: String = "",
                                                driver: String = "com.mysql.cj.jdbc.Driver",
                                                valueSchema: Seq[JdbcPersistenceColumn] = Nil,
                                                valueToSeq: Option[V => Seq[Any]] = None,
                                                sqlToValue: Option[(K, ResultSet) => V] = None,
                                                strToKey: Option[String => K] = None
                                               ) {

  def withUrl(jdbcUrl: String): JdbcPersistenceBuilder[K, V] = copy(url = jdbcUrl)

  def withUserName(jdbcUserName: String): JdbcPersistenceBuilder[K, V] = copy(userName = jdbcUserName)

  def withPassword(jdbcPassword: String): JdbcPersistenceBuilder[K, V] = copy(password = jdbcPassword)

  def withTableName(tableName: String): JdbcPersistenceBuilder[K, V] = copy(suggestedTableName = tableName)

  def withDriver(jdbcDriver: String): JdbcPersistenceBuilder[K, V] = copy(driver = jdbcDriver)

  def withCredentials(jdbcUrl: String, jdbcUserName: String, jdbcPassword: String, tableName: String,
                      jdbcDriver: String): JdbcPersistenceBuilder[K, V] =
    copy(url = jdbcUrl, userName = jdbcUserName, password = jdbcPassword,
      suggestedTableName = tableName, driver = jdbcDriver)

  def withValueSchema(schema: Seq[JdbcPersistenceColumn]): JdbcPersistenceBuilder[K, V] =
    copy(valueSchema = schema)

  def withValueToSeq(f: V => Seq[Any]): JdbcPersistenceBuilder[K, V] =
    copy(valueToSeq = Some(f))

  def withSqlToValue(f: (K, ResultSet) => V): JdbcPersistenceBuilder[K, V] =
    copy(sqlToValue = Some(f))

  def withStrToKey(f: String => K): JdbcPersistenceBuilder[K, V] = copy(strToKey = Some(f))

  def build(): JdbcPersistence[K, V] = {
    require(valueToSeq.nonEmpty && sqlToValue.nonEmpty && strToKey.nonEmpty,
      "Empty valueToSeq, sqlToValue or strToKey functions")

    val tableName = withConnection(c => JdbcPersistenceBuilder.ensureTableExists(c, valueSchema, suggestedTableName))
    new JdbcPersistence[K, V](url, userName, password, tableName,
      driver, JdbcPersistenceBuilder.keyColumnName, valueSchema, valueToSeq.get, sqlToValue.get, strToKey.get)
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
    var tableExists = true
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
          tableExists = false
        case Failure(f) => throw f
      }
    } while (incompatibleTable)

    logger.info(s"Using table named $newTableName")

    if (!tableExists) {
      val stmt = connection.prepareStatement(s"CREATE TABLE IF NOT EXISTS $newTableName (" +
        valueSchema.map(col => s"`${col.name}` ${col.spec}").mkString(", ") +
        s", `$keyColumnName` VARCHAR(200) NOT NULL, PRIMARY KEY (`$keyColumnName`))")
      JdbcUtil.cleanJdbcCall(stmt)(_.close())(_.executeUpdate())

      // create index for all value columns
      valueSchema.foreach { col =>
        val stmtIndex = connection.prepareStatement(s"CREATE INDEX index_${newTableName}_${col.name} " +
          s"ON $newTableName (${col.name})")
        Try(JdbcUtil.cleanJdbcCall(stmtIndex)(_.close())(_.executeUpdate())).recoverWith {
          // fix for MySQL:
          // SQLSyntaxErrorException: BLOB/TEXT column 'xxx' used in key specification without a key length
          case _: SQLSyntaxErrorException =>
            val stmtIndexWithSize = connection.prepareStatement(s"CREATE INDEX index_${newTableName}_${col.name} " +
              s"ON $newTableName (${col.name}(40))")
            Try(JdbcUtil.cleanJdbcCall(stmtIndexWithSize)(_.close())(_.executeUpdate()))
        } match {
          case Success(_) =>
          case Failure(f) =>
            logger.warn(s"Failed to create index on column ${col.name}", f)
        }
      }
    }
    newTableName
  }
}
