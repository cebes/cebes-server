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
 * Created by phvu on 30/11/2016.
 */

package io.cebes.server.result

import java.util.UUID

import com.google.common.cache.{CacheBuilder, LoadingCache}
import com.google.common.util.concurrent.UncheckedExecutionException
import com.google.inject.{Inject, Singleton}
import com.typesafe.scalalogging.slf4j.LazyLogging
import io.cebes.persistence.cache.CachePersistenceSupporter
import io.cebes.persistence.jdbc.{JdbcPersistenceBuilder, JdbcPersistenceColumn, TableNames}
import io.cebes.prop.{Prop, Property}
import io.cebes.server.models.{RequestStatus, SerializableResult}
import spray.json._

@Singleton class JdbcResultStorage @Inject()
(@Prop(Property.MYSQL_URL) jdbcUrl: String,
 @Prop(Property.MYSQL_USERNAME) jdbcUsername: String,
 @Prop(Property.MYSQL_PASSWORD) jdbcPassword: String,
 @Prop(Property.MYSQL_DRIVER) jdbcDriver: String,
 @Prop(Property.CACHESPEC_RESULT_STORE) cacheSpec: String) extends ResultStorage with LazyLogging {

  case class Store(response: String, status: String)

  private lazy val jdbcPersistence = JdbcPersistenceBuilder.newBuilder[UUID, Store]()
    .withCredentials(jdbcUrl, jdbcUsername, jdbcPassword, TableNames.RESULT_STORE, jdbcDriver)
    .withValueSchema(Seq(JdbcPersistenceColumn("response", "MEDIUMTEXT"),
      JdbcPersistenceColumn("status", "VARCHAR(50)")))
    .withValueToSeq(s => Seq(s.response, s.status))
    .withSqlToValue(f => Store(f.getString(1), f.getString(2)))
    .build()

  private lazy val cache: LoadingCache[UUID, Store] = {
    val supporter = new CachePersistenceSupporter[UUID, Store](jdbcPersistence)
    CacheBuilder.from(cacheSpec).removalListener(supporter).build[UUID, Store](supporter)
  }

  override def save(serializableResult: SerializableResult): Unit = {
    cache.put(serializableResult.requestId,
      Store(serializableResult.response.map(_.compactPrint).getOrElse(""),
        serializableResult.status.name))
  }

  override def get(requestId: UUID): Option[SerializableResult] = {
    try {
      val r = cache.get(requestId)
      val status = RequestStatus.fromString(r.status) match {
        case Some(s) => s
        case None =>
          throw new IllegalArgumentException(s"Invalid job status (${r.status}) " +
            s"for request ID ${requestId.toString}")
      }
      val response = Option(r.response) match {
        case None => None
        case Some("") => None
        case Some(content) => Some(content.parseJson)
      }
      Some(SerializableResult(requestId, status, response))
    } catch {
      case e@(_: UncheckedExecutionException | _: IllegalArgumentException) =>
        logger.error(s"Failed to get result for request ID $requestId", e)
        None
    }
  }

  /**
    * Remove the given result. Mostly for testing purpose
    */
  private[result] def remove(requestId: UUID): Unit = {
    cache.invalidate(requestId)
    jdbcPersistence.remove(requestId)
  }
}
