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
 * Created by phvu on 23/08/16.
 */

package io.cebes.server.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.google.inject.Inject
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.cebes.auth.AuthService
import io.cebes.df.DataframeService
import io.cebes.storage.StorageService

import scala.io.StdIn

class HttpServer @Inject()(override val authService: AuthService,
                           override val dfService: DataframeService,
                           override val storageService: StorageService)
  extends StrictLogging with Routes with CebesHttpConfig {

  def start(): Unit = {
    implicit val system = ActorSystem("CebesServerApp")
    implicit val materializer = ActorMaterializer()
    import system.dispatcher

    val bindingFuture = Http().bindAndHandle(routes, httpInterface, httpPort)

    logger.info(s"RESTful server started on $httpInterface:$httpPort")
    StdIn.readChar()

    bindingFuture
      .flatMap(_.unbind())
      .onComplete { _ =>
        system.terminate()
        logger.info("RESTful server stopped")
      }
    System.exit(0)
  }
}
