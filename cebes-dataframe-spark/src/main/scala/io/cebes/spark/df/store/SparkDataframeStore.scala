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
 * Created by phvu on 15/12/2016.
 */

package io.cebes.spark.df.store

import java.util.UUID

import com.google.common.cache.{CacheBuilder, LoadingCache}
import com.google.common.util.concurrent.UncheckedExecutionException
import com.google.inject.{Inject, Singleton}
import com.typesafe.scalalogging.LazyLogging
import io.cebes.df.Dataframe
import io.cebes.df.schema.Schema
import io.cebes.df.store.{DataframeStore, TagStore}
import io.cebes.json.CebesCoreJsonProtocol._
import io.cebes.persistence.cache.CachePersistenceSupporter
import io.cebes.persistence.jdbc.{JdbcPersistenceBuilder, JdbcPersistenceColumn, TableNames}
import io.cebes.prop.types.MySqlBackendCredentials
import io.cebes.prop.{Prop, Property}
import io.cebes.spark.config.HasSparkSession
import io.cebes.spark.df.{SparkDataframe, SparkDataframeFactory}
import org.apache.spark.sql.SaveMode
import spray.json._

/**
  * An implementation of [[DataframeStore]] for Spark,
  * using guava's [[LoadingCache]] with JDBC persistence backend
  */
@Singleton class SparkDataframeStore @Inject()
(@Prop(Property.CACHESPEC_RESULT_STORE) cacheSpec: String,
 mySqlCreds: MySqlBackendCredentials,
 hasSparkSession: HasSparkSession,
 dfFactory: SparkDataframeFactory,
 tagStore: TagStore) extends DataframeStore with LazyLogging {

  private val session = hasSparkSession.session

  private lazy val jdbcPersistence = JdbcPersistenceBuilder.newBuilder[UUID, Dataframe]()
    .withCredentials(mySqlCreds.url, mySqlCreds.userName,
      mySqlCreds.password, TableNames.DF_STORE, mySqlCreds.driver)
    .withValueSchema(Seq(JdbcPersistenceColumn("created_at", "LONG"),
      JdbcPersistenceColumn("table_name", "VARCHAR(200)"),
      JdbcPersistenceColumn("schema", "MEDIUMTEXT")))
    .withValueToSeq { df =>
      SparkDataframeStore.persist(df)
      Seq(System.currentTimeMillis(), SparkDataframeStore.hiveTableName(df.id),
        df.schema.toJson.compactPrint)
    }
    .withSqlToValue { (id, entry) =>
      val sparkDf = session.table(entry.getString(2))
      val schema = entry.getString(3).parseJson.convertTo[Schema]
      dfFactory.df(sparkDf, schema, id)
    }
    .withStrToKey(UUID.fromString)
    .build()

  private lazy val cache: LoadingCache[UUID, Dataframe] = {
    val supporter = new CachePersistenceSupporter[UUID, Dataframe](jdbcPersistence)
      .withRemovalFilter { (id, _) => shouldPersist(id) }
    CacheBuilder.from(cacheSpec).removalListener(supporter).build[UUID, Dataframe](supporter)
  }

  /**
    * Check whether the Dataframe with the given ID should be persisted or not.
    * This is the only place where [[DataframeStore]] depends on [[TagStore]].
    */
  private def shouldPersist(dfId: UUID) = tagStore.find(dfId).nonEmpty

  /////////////////////////////////////////////////////////////////////////////
  // override
  /////////////////////////////////////////////////////////////////////////////

  /**
    * Store the dataframe. If there is already a Dataframe with the same key,
    * it will be overwritten.
    */
  override def add(dataframe: Dataframe): Dataframe = {
    cache.put(dataframe.id, dataframe)
    dataframe
  }

  /**
    * Get the Dataframe with the given ID, if any
    */
  override def get(id: UUID): Option[Dataframe] = {
    try {
      Some(cache.get(id))
    } catch {
      case e@(_: UncheckedExecutionException | _: IllegalArgumentException) =>
        logger.warn(s"Failed to get Dataframe for ID $id: ${e.getMessage}")
        None
    }
  }

  override def persist(dataframe: Dataframe): Unit = {
    jdbcPersistence.upsert(dataframe.id, dataframe)
  }

  override def unpersist(dfId: UUID): Option[Dataframe] = {
    val optionDf = get(dfId)
    if (optionDf.nonEmpty && !shouldPersist(dfId)) {
      // delete from the jdbc persistence
      jdbcPersistence.remove(dfId)
      // drop hive table
      session.sql(s"DROP TABLE IF EXISTS ${SparkDataframeStore.hiveTableName(dfId)}")
    }
    optionDf
  }
}

object SparkDataframeStore {

  private def hiveTableName(dfId: UUID) = s"spark_${dfId.toString.replace("-", "_")}"

  private def persist(dataframe: Dataframe): Unit = {
    val sparkDf = dataframe match {
      case d: SparkDataframe => d
      case _ => throw new IllegalArgumentException("Only SparkDataframe is accepted")
    }
    sparkDf.sparkDf.write.mode(SaveMode.Overwrite).saveAsTable(hiveTableName(sparkDf.id))
  }
}
