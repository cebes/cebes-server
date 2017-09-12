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
package io.cebes.http.server

import java.util.concurrent.TimeUnit

import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.cebes.http.server.routes.{AkkaImplicits, SecuredSession}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

trait HttpServer extends AkkaImplicits with SecuredSession with LazyLogging {

  val httpInterface: String

  val httpPort: Int

  protected val routes: Route

  ////////////////////////////////////////////////////////////////////////////


  private var bindingFuture: Future[Http.ServerBinding] = _

  /**
    * Start the http service
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
    Await.result(actorSystem.whenTerminated, Duration.Inf)
  }
}
