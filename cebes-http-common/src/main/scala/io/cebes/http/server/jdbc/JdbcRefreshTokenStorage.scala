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
package io.cebes.http.server.jdbc

import com.softwaremill.session.{RefreshTokenData, RefreshTokenLookupResult, RefreshTokenStorage}
import io.cebes.http.server.routes.SessionData
import io.cebes.persistence.jdbc.{JdbcPersistence, JdbcPersistenceBuilder, JdbcPersistenceColumn}
import io.cebes.prop.types.MySqlBackendCredentials

import scala.concurrent.Future
import scala.concurrent.duration.Duration

trait JdbcRefreshTokenStorage extends RefreshTokenStorage[SessionData] {

  protected val mySqlCreds: MySqlBackendCredentials
  protected val tableName: String

  case class Store(userName: String, tokenHash: String, expires: Long)

  private lazy val persistence: JdbcPersistence[String, Store] =
    JdbcPersistenceBuilder.newBuilder[String, Store]()
      .withCredentials(mySqlCreds.url, mySqlCreds.userName,
        mySqlCreds.password, tableName, mySqlCreds.driver)
      .withValueSchema(Seq(
        JdbcPersistenceColumn("user_name", "VARCHAR(200)"),
        JdbcPersistenceColumn("token_hash", "VARCHAR(255)"),
        JdbcPersistenceColumn("expires", "BIGINT")))
      .withValueToSeq(v => Seq(v.userName, v.tokenHash, v.expires))
      .withSqlToValue {
        case (_, r) => Store(r.getString(1), r.getString(2), r.getLong(3))
      }
      .withStrToKey(s => s)
      .build()

  override def lookup(selector: String): Future[Option[RefreshTokenLookupResult[SessionData]]] = Future.successful {
    persistence.get(selector).map { s =>
      RefreshTokenLookupResult[SessionData](s.tokenHash, s.expires, () => SessionData(s.userName))
    }
  }

  override def store(data: RefreshTokenData[SessionData]): Future[Unit] = {
    Future.successful(persistence.upsert(data.selector, Store(data.forSession.userName, data.tokenHash, data.expires)))
  }

  override def remove(selector: String): Future[Unit] = {
    Future.successful(persistence.remove(selector))
  }

  override def schedule[S](after: Duration)(op: => Future[S]): Unit = {
    op
    Future.successful(())
  }
}
