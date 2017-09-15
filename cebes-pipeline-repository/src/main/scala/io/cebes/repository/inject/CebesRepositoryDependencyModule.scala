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
package io.cebes.repository.inject

import com.google.inject.AbstractModule
import io.cebes.auth.AuthService
import io.cebes.auth.simple.SimpleAuthService
import io.cebes.repository.PipelineRepositoryService
import io.cebes.repository.db.SimplePipelineRepositoryService

class CebesRepositoryDependencyModule extends AbstractModule {
  override def configure(): Unit = {
    bind(classOf[AuthService]).to(classOf[SimpleAuthService])
    bind(classOf[PipelineRepositoryService]).to(classOf[SimplePipelineRepositoryService])
  }
}
