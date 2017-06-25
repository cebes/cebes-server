/* Copyright 2017 The Cebes Authors. All Rights Reserved.
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
package io.cebes.spark.config

import com.google.inject.{Inject, Injector, Provider}
import io.cebes.df.Dataframe
import io.cebes.pipeline.ml.Model
import io.cebes.pipeline.models.Pipeline
import io.cebes.prop.{Prop, Property}
import io.cebes.spark.df.store.{SparkDataframeTagStore, SparkDataframeStore}
import io.cebes.spark.pipeline.store.{SparkModelTagStore, SparkPipelineTagStore, SparkModelStore, SparkPipelineStore}
import io.cebes.store.{CachedStore, TagStore}

class HasSparkSessionProvider @Inject()
(@Prop(Property.SPARK_MODE) val sparkMode: String,
 val injector: Injector) extends Provider[HasSparkSession] {

  override def get(): HasSparkSession = {
    sparkMode.toLowerCase match {
      case "local" => injector.getInstance(classOf[HasSparkSessionLocal])
      case "yarn" => injector.getInstance(classOf[HasSparkSessionYarn])
      case _ => throw new IllegalArgumentException(s"Invalid spark mode: $sparkMode")
    }
  }
}

class CachedStoreDataframeProvider @Inject()(injector: Injector) extends Provider[CachedStore[Dataframe]] {

  override def get(): CachedStore[Dataframe] = injector.getInstance(classOf[SparkDataframeStore])
}

class TagStoreDataframeProvider @Inject()(injector: Injector) extends Provider[TagStore[Dataframe]] {

  override def get(): TagStore[Dataframe] = injector.getInstance(classOf[SparkDataframeTagStore])
}

class CachedStorePipelineProvider @Inject()(injector: Injector) extends Provider[CachedStore[Pipeline]] {

  override def get(): CachedStore[Pipeline] = injector.getInstance(classOf[SparkPipelineStore])
}

class TagStorePipelineProvider @Inject()(injector: Injector) extends Provider[TagStore[Pipeline]] {

  override def get(): TagStore[Pipeline] = injector.getInstance(classOf[SparkPipelineTagStore])
}

class CachedStoreModelProvider @Inject()(injector: Injector) extends Provider[CachedStore[Model]] {

  override def get(): CachedStore[Model] = injector.getInstance(classOf[SparkModelStore])
}

class TagStoreModelProvider @Inject()(injector: Injector) extends Provider[TagStore[Model]] {

  override def get(): TagStore[Model] = injector.getInstance(classOf[SparkModelTagStore])
}
