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

import com.google.common.cache.LoadingCache
import com.google.inject.{Inject, Singleton}
import io.cebes.df.Dataframe
import io.cebes.df.schema.Schema
import io.cebes.json.CebesCoreJsonProtocol._
import io.cebes.persistence.jdbc.{JdbcPersistence, JdbcPersistenceBuilder, JdbcPersistenceColumn, TableNames}
import io.cebes.persistence.store.JdbcCachedStore
import io.cebes.prop.types.MySqlBackendCredentials
import io.cebes.prop.{Prop, Property}
import io.cebes.spark.config.HasSparkSession
import io.cebes.spark.df.{SparkDataframe, SparkDataframeFactory}
import io.cebes.store.{CachedStore, TagStore}
import org.apache.spark.sql.SaveMode
import spray.json._

/**
  * An implementation of [[CachedStore[Dataframe]]] for Spark,
  * using guava's [[LoadingCache]] with JDBC persistence backend
  */
@Singleton class SparkDataframeStore @Inject()
(@Prop(Property.CACHESPEC_DF_STORE) cacheSpec: String,
 mySqlCreds: MySqlBackendCredentials,
 hasSparkSession: HasSparkSession,
 dfFactory: SparkDataframeFactory,
 tagStore: TagStore[Dataframe]) extends JdbcCachedStore[Dataframe](cacheSpec) {

  private val session = hasSparkSession.session

  /**
    * The JDBC persistence that backs the LoadingCache.
    */
  override protected lazy val jdbcPersistence: JdbcPersistence[UUID, Dataframe] =
    JdbcPersistenceBuilder.newBuilder[UUID, Dataframe]()
      .withCredentials(mySqlCreds.url, mySqlCreds.userName,
        mySqlCreds.password, TableNames.DF_STORE, mySqlCreds.driver)
      .withValueSchema(Seq(JdbcPersistenceColumn("created_at", "BIGINT"),
        JdbcPersistenceColumn("table_name", "VARCHAR(200)"),
        JdbcPersistenceColumn("df_schema", "MEDIUMTEXT")))
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

  /**
    * Check whether the object with the given ID should be persisted or not.
    * This is the only place where [[SparkDataframeStore]] depends on [[TagStore[Dataframe]]].
    */
  override protected def shouldPersist(id: UUID): Boolean = tagStore.find(id).nonEmpty

  /** Custom logic that persist the object, if needed by the subclass */
  override protected def doPersist(obj: Dataframe): Unit = {
    SparkDataframeStore.persist(obj)
  }

  /** Custom logic to unpersist the object, if needed by the subclass */
  override protected def doUnpersist(obj: Dataframe): Unit = {
    // drop hive table
    session.sql(s"DROP TABLE IF EXISTS ${SparkDataframeStore.hiveTableName(obj.id)}")
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
