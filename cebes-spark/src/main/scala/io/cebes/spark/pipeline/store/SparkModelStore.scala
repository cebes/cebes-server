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
package io.cebes.spark.pipeline.store

import java.util.UUID

import com.google.inject.{Inject, Singleton}
import io.cebes.persistence.jdbc.{JdbcPersistence, JdbcPersistenceBuilder, JdbcPersistenceColumn, TableNames}
import io.cebes.persistence.store.JdbcCachedStore
import io.cebes.pipeline.factory.ModelFactory
import io.cebes.pipeline.json.ModelDef
import io.cebes.pipeline.ml.Model
import io.cebes.prop.types.MySqlBackendCredentials
import io.cebes.prop.{Prop, Property}
import io.cebes.spark.json.CebesSparkJsonProtocol._
import io.cebes.store.{CachedStore, TagStore}
import spray.json._

/**
  * An implementation of [[CachedStore[Pipeline]]] for Spark,
  * using guava's LoadingCache with JDBC persistence backend
  */
@Singleton class SparkModelStore @Inject()
(@Prop(Property.CACHESPEC_MODEL_STORE) val cacheSpec: String,
 mySqlCreds: MySqlBackendCredentials,
 modelFactory: ModelFactory,
 tagStore: TagStore[Model]) extends JdbcCachedStore[Model](cacheSpec) {

  /**
    * The JDBC persistence that backs the LoadingCache.
    * To be defined by the subclasses
    */
  override protected lazy val jdbcPersistence: JdbcPersistence[UUID, Model] =
    JdbcPersistenceBuilder.newBuilder[UUID, Model]()
      .withCredentials(mySqlCreds.url, mySqlCreds.userName,
        mySqlCreds.password, TableNames.MODEL_STORE, mySqlCreds.driver)
      .withValueSchema(Seq(JdbcPersistenceColumn("created_at", "BIGINT"),
        JdbcPersistenceColumn("model_def", "MEDIUMTEXT")))
      .withValueToSeq { model =>
        Seq(System.currentTimeMillis(), modelFactory.save(model).toJson.compactPrint)
      }
      .withSqlToValue { (_, entry) =>
        val modelDef = entry.getString(2).parseJson.convertTo[ModelDef]
        modelFactory.create(modelDef)
      }
      .withStrToKey(s => UUID.fromString(s))
      .build()

  /** Check whether the object with the given ID should be persisted or not. */
  override protected def shouldPersist(id: UUID): Boolean = tagStore.find(id).nonEmpty

  /** Custom logic that persist the object, if needed by the subclass */
  override protected def doPersist(obj: Model): Unit = {
    // do nothing
  }

  /** Custom logic to unpersist the object, if needed by the subclass */
  override protected def doUnpersist(obj: Model): Unit = {
    // do nothing
  }
}
