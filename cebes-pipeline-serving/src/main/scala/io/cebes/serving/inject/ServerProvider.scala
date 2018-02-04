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

import com.google.inject.{Inject, Injector, Provider}
import io.cebes.http.server.HttpServer
import io.cebes.pipeline.json.ServingConfiguration
import io.cebes.serving.http.{CebesServingSecuredServer, CebesServingServer}

/**
  * Provider of a server, depending on whether it is configured to be secured or not
  */
class ServerProvider @Inject()(private val servingConfiguration: ServingConfiguration,
                               private val injector: Injector) extends Provider[HttpServer] {

  override def get(): HttpServer = {
    if (servingConfiguration.secured) {
      injector.getInstance(classOf[CebesServingSecuredServer])
    } else {
      injector.getInstance(classOf[CebesServingServer])
    }
  }
}
