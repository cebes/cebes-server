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
 * Created by phvu on 06/09/16.
 */

package io.cebes.spark

import com.google.inject.{AbstractModule, Singleton, TypeLiteral}
import io.cebes.df.{Dataframe, DataframeService}
import io.cebes.pipeline.models.{Pipeline, PipelineMessageSerializer}
import io.cebes.spark.config._
import io.cebes.spark.df.SparkDataframeService
import io.cebes.spark.pipeline.SparkPipelineMessageSerializer
import io.cebes.store.{CachedStore, TagStore}


class CebesSparkDependencyModule extends AbstractModule {

  protected def configure(): Unit = {
    bind(classOf[HasSparkSession]).toProvider(classOf[HasSparkSessionProvider])
    bind(new TypeLiteral[CachedStore[Dataframe]]() {})
      .toProvider(classOf[CachedStoreDataframeProvider]).in(classOf[Singleton])
    bind(new TypeLiteral[TagStore[Dataframe]]() {})
      .toProvider(classOf[TagStoreDataframeProvider]).in(classOf[Singleton])
    bind(new TypeLiteral[CachedStore[Pipeline]]() {})
      .toProvider(classOf[CachedStorePipelineProvider]).in(classOf[Singleton])
    bind(new TypeLiteral[TagStore[Pipeline]]() {})
      .toProvider(classOf[TagStorePipelineProvider]).in(classOf[Singleton])

    bind(classOf[DataframeService]).to(classOf[SparkDataframeService])
    bind(classOf[PipelineMessageSerializer]).to(classOf[SparkPipelineMessageSerializer])
  }
}
