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

package io.cebes.http.server.jdbc

import java.util.UUID

import com.google.common.cache.{CacheBuilder, LoadingCache}
import com.google.common.util.concurrent.UncheckedExecutionException
import com.typesafe.scalalogging.LazyLogging
import io.cebes.http.server.result.ResultStorage
import io.cebes.http.server.{RequestStatuses, SerializableResult}
import io.cebes.persistence.cache.CachePersistenceSupporter
import io.cebes.persistence.jdbc.{JdbcPersistenceBuilder, JdbcPersistenceColumn}
import io.cebes.prop.types.MySqlBackendCredentials
import spray.json._

/**
  * Trait for storing results of asynchronous commands.
  * Subclasses of this trait should be singleton
  */
trait JdbcResultStorage extends ResultStorage with LazyLogging {

  protected val cacheSpec: String
  protected val mySqlCreds: MySqlBackendCredentials
  protected val tableName: String

  case class Store(createdAt: Long, requestUri: String, requestEntity: String, status: String, response: String)

  private lazy val jdbcPersistence = JdbcPersistenceBuilder.newBuilder[UUID, Store]()
    .withCredentials(mySqlCreds.url, mySqlCreds.userName,
      mySqlCreds.password, tableName, mySqlCreds.driver)
    .withValueSchema(Seq(JdbcPersistenceColumn("created_at", "BIGINT"),
      JdbcPersistenceColumn("request_uri", "VARCHAR(255)"),
      JdbcPersistenceColumn("request_entity", "MEDIUMTEXT"),
      JdbcPersistenceColumn("status", "VARCHAR(50)"),
      JdbcPersistenceColumn("response", "MEDIUMTEXT")))
    .withValueToSeq(s => Seq(s.createdAt, s.requestUri, s.requestEntity, s.status, s.response))
    .withSqlToValue {
      case (_, f) => Store(f.getLong(1), f.getString(2), f.getString(3), f.getString(4), f.getString(5))
    }
    .withStrToKey(UUID.fromString)
    .build()

  private lazy val cache: LoadingCache[UUID, Store] = {
    val supporter = new CachePersistenceSupporter[UUID, Store](jdbcPersistence)
    CacheBuilder.from(cacheSpec).removalListener(supporter).build[UUID, Store](supporter)
  }

  override def save(serializableResult: SerializableResult): Unit = {
    cache.put(serializableResult.requestId,
      Store(System.currentTimeMillis,
        serializableResult.requestUri,
        serializableResult.requestEntity.map(_.compactPrint).getOrElse(""),
        serializableResult.status.name,
        serializableResult.response.map(_.compactPrint).getOrElse("")
      ))
  }

  override def get(requestId: UUID): Option[SerializableResult] = {
    try {
      val r = cache.get(requestId)
      val status = RequestStatuses.fromString(r.status) match {
        case Some(s) => s
        case None =>
          throw new IllegalArgumentException(s"Invalid job status (${r.status}) " +
            s"for request ID ${requestId.toString}")
      }
      val response = parseOptionalJsValue(r.response)
      val requestEntity = parseOptionalJsValue(r.requestEntity)
      Some(SerializableResult(requestId, r.requestUri, requestEntity, status, response))
    } catch {
      case e@(_: UncheckedExecutionException | _: IllegalArgumentException) =>
        logger.warn(s"Failed to get result for request ID $requestId: ${e.getMessage}")
        None
    }
  }

  /**
    * Remove the given result. Mostly for testing purpose
    */
  private[jdbc] def remove(requestId: UUID): Unit = {
    cache.invalidate(requestId)
    jdbcPersistence.remove(requestId)
  }

  /**
    * Private helper to parse optional JSON object
    */
  private def parseOptionalJsValue(s: String): Option[JsValue] = Option(s) match {
    case None => None
    case Some("") => None
    case Some(content) => Some(content.parseJson)
  }
}
