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
package io.cebes.serving.inject

import com.google.inject.{AbstractModule, Singleton}
import io.cebes.serving.common.{ServingConfigurationProvider, ServingManager}
import io.cebes.serving.spark.{SparkPipelineServingService, SparkServingManager}
import io.cebes.serving.{PipelineServingService, ServingConfiguration}

class CebesServingDependencyModule extends AbstractModule {
  override def configure(): Unit = {
    bind(classOf[ServingConfiguration]).toProvider(classOf[ServingConfigurationProvider])
    bind(classOf[PipelineServingService]).to(classOf[SparkPipelineServingService])
    bind(classOf[ServingManager]).to(classOf[SparkServingManager]).in(classOf[Singleton])
  }
}
