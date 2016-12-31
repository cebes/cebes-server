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
 * Created by phvu on 31/12/2016.
 */

package io.cebes.persistence.jdbc

import java.sql._

import com.typesafe.scalalogging.LazyLogging
import io.cebes.persistence.ClosableIterator

class ResultSetIterable[T](connection: Connection, stmt: PreparedStatement,
                           resultSetFn: ResultSet => T) extends ClosableIterator[T] with LazyLogging {

  private val resultSet = stmt.executeQuery()

  override def hasNext: Boolean = {
    val v = resultSet.next()
    if (v) {
      resultSet.previous()
    }
    v
  }

  override def next(): T = {
    resultSet.next()
    resultSetFn(resultSet)
  }

  private def safeClose(a: AutoCloseable): Unit = {
    if (a != null) {
      try {
        a.close()
      } catch {
        case ex: SQLException =>
          logger.error(s"Failed to close resource: ${ex.getMessage}")
      }
    }
  }

  override def close(): Unit = {
    safeClose(resultSet)
    safeClose(stmt)
    safeClose(connection)
  }
}
