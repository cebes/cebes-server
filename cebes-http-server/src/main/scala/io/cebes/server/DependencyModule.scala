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

package io.cebes.server

import com.google.inject.{Binder, Guice, Module}
import io.cebes.auth.AuthService
import io.cebes.auth.simple.SimpleAuthService
import io.cebes.df.DataframeService
import io.cebes.spark.df.SparkDataframeService

/**
  * Guice's configuration class that is defining the interface-implementation bindings
  */
class DependencyModule extends Module {
  def configure(binder: Binder): Unit = {
    binder.bind(classOf[AuthService]).to(classOf[SimpleAuthService])
    binder.bind(classOf[DataframeService]).to(classOf[SparkDataframeService])
  }
}

object InjectorService {
  val injector = Guice.createInjector(new DependencyModule)
}
