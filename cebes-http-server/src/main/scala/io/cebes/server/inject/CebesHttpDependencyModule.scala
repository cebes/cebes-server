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
 * Created by phvu on 24/08/16.
 */

package io.cebes.server.inject

import com.google.inject.AbstractModule
import io.cebes.auth.AuthService
import io.cebes.auth.simple.SimpleAuthService
import io.cebes.df.DataframeService
import io.cebes.server.result.{JdbcResultStorage, ResultStorage}
import io.cebes.spark.df.SparkDataframeService
import io.cebes.spark.storage.SparkStorageService
import io.cebes.storage.{DataWriter, StorageService}

/**
  * Guice's configuration class that is defining the interface-implementation bindings
  */
class CebesHttpDependencyModule extends AbstractModule {

  protected override def configure(): Unit = {
    bind(classOf[AuthService]).to(classOf[SimpleAuthService])
    bind(classOf[DataframeService]).to(classOf[SparkDataframeService])
    bind(classOf[StorageService]).to(classOf[SparkStorageService])
    bind(classOf[DataWriter]).toProvider(classOf[DataWriterProvider])
    bind(classOf[ResultStorage]).to(classOf[JdbcResultStorage])
  }
}
