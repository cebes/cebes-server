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
import io.cebes.pipeline.ModelService
import io.cebes.pipeline.factory.ModelFactory
import io.cebes.pipeline.ml.Model
import io.cebes.pipeline.models.{Pipeline, PipelineMessageSerializer}
import io.cebes.spark.config._
import io.cebes.spark.df.SparkDataframeService
import io.cebes.spark.df.store.{SparkDataframeStore, SparkDataframeTagStore}
import io.cebes.spark.pipeline.store.{SparkModelStore, SparkModelTagStore, SparkPipelineStore, SparkPipelineTagStore}
import io.cebes.spark.pipeline.{SparkModelFactory, SparkModelService, SparkPipelineMessageSerializer}
import io.cebes.spark.storage.DataWriterProvider
import io.cebes.storage.DataWriter
import io.cebes.store.{CachedStore, TagStore}


class CebesSparkDependencyModule extends AbstractModule {

  protected def configure(): Unit = {
    bind(classOf[HasSparkSession]).toProvider(classOf[HasSparkSessionProvider])
    bind(classOf[DataWriter]).toProvider(classOf[DataWriterProvider])

    bind(new TypeLiteral[CachedStore[Dataframe]]() {})
      .to(classOf[SparkDataframeStore]).in(classOf[Singleton])
    bind(new TypeLiteral[TagStore[Dataframe]]() {})
      .to(classOf[SparkDataframeTagStore]).in(classOf[Singleton])
    bind(new TypeLiteral[CachedStore[Pipeline]]() {})
      .to(classOf[SparkPipelineStore]).in(classOf[Singleton])
    bind(new TypeLiteral[TagStore[Pipeline]]() {})
      .to(classOf[SparkPipelineTagStore]).in(classOf[Singleton])
    bind(new TypeLiteral[CachedStore[Model]]() {})
      .to(classOf[SparkModelStore]).in(classOf[Singleton])
    bind(new TypeLiteral[TagStore[Model]]() {})
      .to(classOf[SparkModelTagStore]).in(classOf[Singleton])

    bind(classOf[DataframeService]).to(classOf[SparkDataframeService])
    bind(classOf[ModelService]).to(classOf[SparkModelService])
    bind(classOf[ModelFactory]).to(classOf[SparkModelFactory])
    bind(classOf[PipelineMessageSerializer]).to(classOf[SparkPipelineMessageSerializer])
  }
}
