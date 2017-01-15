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

import com.google.inject.{Inject, Singleton}
import io.cebes.df.store.PipelineTagStore
import io.cebes.persistence.jdbc.TableNames
import io.cebes.persistence.store.JdbcTagStore
import io.cebes.prop.types.MySqlBackendCredentials


/**
  * An implementation of [[io.cebes.df.store.PipelineTagStore]] for Pipelines,
  * with JDBC persistence backend.
  */
@Singleton class JdbcPipelineTagStore @Inject()
(mySqlCreds: MySqlBackendCredentials)
  extends JdbcTagStore(mySqlCreds, TableNames.PIPELINE_TAG_STORE) with PipelineTagStore
