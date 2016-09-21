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

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.google.inject.Inject
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.cebes.auth.AuthService
import io.cebes.df.DataframeService
import io.cebes.prop.{Prop, Property}
import io.cebes.server.result.ResultStorage
import io.cebes.server.routes.Routes
import io.cebes.storage.StorageService

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.io.StdIn

class HttpServer @Inject()(override val authService: AuthService,
                           override val dfService: DataframeService,
                           override val storageService: StorageService,
                           override val resultStorage: ResultStorage,
                           @Prop(Property.HTTP_INTERFACE) val httpInterface: String,
                           @Prop(Property.HTTP_PORT) val httpPort: Int)
  extends StrictLogging with Routes {

  implicit val actorSystem = ActorSystem("CebesServerApp")
  implicit val actorExecutor = actorSystem.dispatcher
  implicit val actorMaterializer = ActorMaterializer()

  var bindingFuture: Future[Http.ServerBinding] = _

  /**
    * Start the Cebes http service
    *
    */
  def start(): Unit = {
    bindingFuture = Http().bindAndHandle(routes, httpInterface, httpPort)
    logger.info(s"RESTful server started on $httpInterface:$httpPort")
  }

  def stop(): Unit = {
    bindingFuture.flatMap(_.unbind()).onComplete { _ =>
      actorSystem.terminate()
      Await.result(actorSystem.whenTerminated, Duration(10, TimeUnit.SECONDS))
      logger.info("RESTful server stopped")
    }
  }

  def waitServer(): Unit = {
    //Await.result(actorSystem.whenTerminated, Duration.Inf)
    logger.info("Press enter to stop")
    StdIn.readLine()
  }
}
