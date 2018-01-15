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
package io.cebes.repository.client

import com.google.inject.Inject
import io.cebes.pipeline.factory.PipelineFactory
import io.cebes.prop.{Prop, Property}

/**
  * A factory for creating [[RepositoryClient]]
  */
class RepositoryClientFactory @Inject()
(private val pplFactory: PipelineFactory,
 @Prop(Property.DEFAULT_REPOSITORY_HOST) private val systemDefaultRepoHost: String,
 @Prop(Property.DEFAULT_REPOSITORY_PORT) private val systemDefaultRepoPort: Int) {

  def get(userName: Option[String], passwordHash: Option[String], authToken: Option[String]): RepositoryClient = {
    new RepositoryClient(pplFactory, systemDefaultRepoHost, systemDefaultRepoPort,
      userName, passwordHash, authToken)
  }
}
