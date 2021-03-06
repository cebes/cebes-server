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
 * Created by phvu on 09/09/16.
 */

package io.cebes.server.helpers

import com.google.inject.{Guice, Injector, Stage}
import io.cebes.prop.PropertyModule
import io.cebes.server.inject.CebesHttpDependencyModule
import io.cebes.spark.CebesSparkDependencyModule

import scala.reflect.ClassTag

object CebesHttpServerTestInjector {

  private lazy val injector: Injector = Guice.createInjector(Stage.DEVELOPMENT,
    new PropertyModule(true),
    new CebesHttpDependencyModule,
    new CebesSparkDependencyModule)


  def instance[T](implicit tag: ClassTag[T]):T = injector.getInstance(tag.runtimeClass.asInstanceOf[Class[T]])
}
