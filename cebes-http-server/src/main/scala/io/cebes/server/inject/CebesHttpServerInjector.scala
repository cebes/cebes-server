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

package io.cebes.server.inject

import com.google.inject.{Guice, Injector, Stage}
import io.cebes.prop.PropertyModule
import io.cebes.spark.CebesSparkDependencyModule

import scala.reflect.ClassTag

object CebesHttpServerInjector {

  private lazy val injector: Injector = Guice.createInjector(Stage.PRODUCTION,
    new PropertyModule(false),
    new CebesHttpDependencyModule,
    new CebesSparkDependencyModule)

  /**
    * Short-hand for getting an instance from the DI framework
    *
    * @tparam T type of the class to obtain an instance
    * @return instance of type T
    */
  def instance[T](implicit tag: ClassTag[T]): T = injector.getInstance(tag.runtimeClass.asInstanceOf[Class[T]])
}
