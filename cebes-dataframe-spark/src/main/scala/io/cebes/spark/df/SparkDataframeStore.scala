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

package io.cebes.spark.df

import java.util.UUID

import com.google.common.cache.{CacheBuilder, LoadingCache}
import com.google.common.util.concurrent.UncheckedExecutionException
import com.google.inject.{Inject, Singleton}
import com.typesafe.scalalogging.LazyLogging
import io.cebes.df.schema.Schema
import io.cebes.df.{Dataframe, DataframeStore}
import io.cebes.json.CebesCoreJsonProtocol._
import io.cebes.persistence.cache.CachePersistenceSupporter
import io.cebes.persistence.jdbc.{JdbcPersistenceBuilder, JdbcPersistenceColumn, TableNames}
import io.cebes.prop.{Prop, Property}
import io.cebes.spark.config.HasSparkSession
import org.apache.spark.sql.SaveMode
import spray.json._

@Singleton class SparkDataframeStore @Inject()
(@Prop(Property.MYSQL_URL) jdbcUrl: String,
 @Prop(Property.MYSQL_USERNAME) jdbcUsername: String,
 @Prop(Property.MYSQL_PASSWORD) jdbcPassword: String,
 @Prop(Property.MYSQL_DRIVER) jdbcDriver: String,
 @Prop(Property.CACHESPEC_RESULT_STORE) cacheSpec: String,
 hasSparkSession: HasSparkSession,
 dfFactory: SparkDataframeFactory) extends DataframeStore with LazyLogging {

  private val session = hasSparkSession.session

  private lazy val jdbcPersistence = JdbcPersistenceBuilder.newBuilder[UUID, Dataframe]()
    .withCredentials(jdbcUrl, jdbcUsername, jdbcPassword, TableNames.DF_STORE, jdbcDriver)
    .withValueSchema(Seq(JdbcPersistenceColumn("created_at", "LONG"),
      JdbcPersistenceColumn("table_name", "VARCHAR(200)"),
      JdbcPersistenceColumn("schema", "MEDIUMTEXT")))
    .withValueToSeq { df =>
      val sparkDf = df match {
        case d: SparkDataframe => d
        case _ => throw new IllegalArgumentException("Only SparkDataframe is accepted")
      }
      val tbName = s"spark_${sparkDf.id.toString}"
      sparkDf.sparkDf.write.mode(SaveMode.Overwrite).saveAsTable(tbName)
      Seq(System.currentTimeMillis(), tbName, sparkDf.schema.toJson.compactPrint)
    }
    .withSqlToValue { case (id, entry) =>
      val sparkDf = session.table(entry.getString(2))
      val schema = entry.getString(3).parseJson.convertTo[Schema]
      dfFactory.df(sparkDf, schema, id)
    }
    .build()

  private lazy val cache: LoadingCache[UUID, Dataframe] = {
    val supporter = new CachePersistenceSupporter[UUID, Dataframe](jdbcPersistence)
      .withRemovalFilter { case (_, _) =>
        //TODO: implement this: only store dataframes that are pinned (and tests)
        true
      }
    CacheBuilder.from(cacheSpec).removalListener(supporter).build[UUID, Dataframe](supporter)
  }

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
}
