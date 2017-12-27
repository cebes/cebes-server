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
import io.cebes.auth.AuthService
import io.cebes.auth.simple.SimpleAuthService
import io.cebes.http.server.HttpServer
import io.cebes.http.server.result.ResultStorage
import io.cebes.pipeline.json.ServingConfiguration
import io.cebes.pipeline.{InferenceManager, InferenceService}
import io.cebes.serving.common.DefaultInferenceService
import io.cebes.serving.http.CebesServingResultStorage

class TestDependencyModule extends AbstractModule {

  override def configure(): Unit = {
    bind(classOf[ServingConfiguration]).toProvider(classOf[ServingConfigurationProvider])
    bind(classOf[InferenceService]).to(classOf[DefaultInferenceService])
    bind(classOf[InferenceManager]).to(classOf[TestInferenceManager]).in(classOf[Singleton])

    bind(classOf[AuthService]).to(classOf[SimpleAuthService])
    bind(classOf[ResultStorage]).to(classOf[CebesServingResultStorage]).in(classOf[Singleton])
    bind(classOf[HttpServer]).toProvider(classOf[ServerProvider])
  }
}
