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

package io.cebes.server.http

import com.google.inject.Inject
import com.softwaremill.session.{RefreshTokenData, RefreshTokenLookupResult, RefreshTokenStorage}
import io.cebes.persistence.jdbc.{JdbcPersistence, JdbcPersistenceBuilder, JdbcPersistenceColumn}
import io.cebes.prop.{Prop, Property}

import scala.concurrent.Future
import scala.concurrent.duration.Duration

class JdbcRefreshTokenStorage @Inject()
(@Prop(Property.MYSQL_URL) jdbcUrl: String,
 @Prop(Property.MYSQL_USERNAME) jdbcUserName: String,
 @Prop(Property.MYSQL_PASSWORD) jdbcPassword: String,
 @Prop(Property.MYSQL_DRIVER) jdbcDriver: String) extends RefreshTokenStorage[SessionData] {

  case class Store(userName: String, tokenHash: String, expires: Long)

  val persistence: JdbcPersistence[String, Store] =
    JdbcPersistenceBuilder.newBuilder[String, Store]().
      withCredentials(jdbcUrl, jdbcUserName, jdbcPassword, "persistence_refresh_tokens", jdbcDriver).
      withValueSchema(Seq(JdbcPersistenceColumn("userName", "VARCHAR (200)"),
        JdbcPersistenceColumn("tokenHash", "VARCHAR(256)"),
        JdbcPersistenceColumn("expires", "Long"))).
      withValueToSeq(v => Seq(v.userName, v.tokenHash, v.expires)).
      withSqlToValue(r => Store(r.getString("userName"), r.getString("tokenHash"), r.getLong("expires"))).build()

  override def lookup(selector: String): Future[Option[RefreshTokenLookupResult[SessionData]]] = Future.successful {
    persistence.lookup(selector).map { s =>
      RefreshTokenLookupResult[SessionData](s.tokenHash, s.expires, () => SessionData(s.userName))
    }
  }

  override def store(data: RefreshTokenData[SessionData]): Future[Unit] = {
    Future.successful(persistence.store(data.selector, Store(data.forSession.userName, data.tokenHash, data.expires)))
  }

  override def remove(selector: String): Future[Unit] = {
    Future.successful(persistence.remove(selector))
  }

  override def schedule[S](after: Duration)(op: => Future[S]): Unit = {
    op
    Future.successful(())
  }
}
