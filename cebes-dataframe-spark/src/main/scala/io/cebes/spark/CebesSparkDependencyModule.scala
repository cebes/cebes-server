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

import com.google.inject.AbstractModule
import io.cebes.df.store.{DataframeStore, DataframeTagStore}
import io.cebes.pipeline.factory.StageFactory
import io.cebes.pipeline.{PipelineStore, PipelineTagStore}
import io.cebes.spark.config.{HasSparkSession, HasSparkSessionProvider}
import io.cebes.spark.df.store.{JdbcDataframeTagStore, SparkDataframeStore}
import io.cebes.spark.pipeline.store.{JdbcPipelineTagStore, SparkPipelineStore}


class CebesSparkDependencyModule extends AbstractModule {

  protected def configure(): Unit = {
    bind(classOf[HasSparkSession]).toProvider(classOf[HasSparkSessionProvider])
    bind(classOf[DataframeStore]).to(classOf[SparkDataframeStore])
    bind(classOf[DataframeTagStore]).to(classOf[JdbcDataframeTagStore])
    bind(classOf[PipelineStore]).to(classOf[SparkPipelineStore])
    bind(classOf[PipelineTagStore]).to(classOf[JdbcPipelineTagStore])
    bind(classOf[StageFactory]).toInstance(
      new StageFactory(Seq("io.cebes.pipeline.models", "io.cebes.spark.pipeline.etl")))
  }
}
