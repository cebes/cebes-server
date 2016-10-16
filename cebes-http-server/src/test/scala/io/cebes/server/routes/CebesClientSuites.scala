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
 * Created by phvu on 10/10/2016.
 */

package io.cebes.server.routes

import akka.http.scaladsl.model.HttpMethods
import io.cebes.server.helpers.{Client, HttpServerTest}
import io.cebes.server.models._
import io.cebes.server.models.CebesJsonProtocol._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import io.cebes.server.routes.auth.AuthHandlerSuite
import io.cebes.server.routes.storage.StorageHandlerSuite
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.immutable.IndexedSeq

class CebesClientSuites extends Suite with BeforeAndAfterAll {

  val client = new Client

  override def nestedSuites: IndexedSeq[Suite] = IndexedSeq(
    new AuthHandlerSuite(client),
    new StorageHandlerSuite(client))

  /**
    * Before and after all
    */

  override def beforeAll(): Unit = {
    super.beforeAll()
    HttpServerTest.register()
    assert("ok" === client.request[UserLogin, OkResponse](HttpMethods.POST,
      "auth/login", UserLogin("foo", "bar")).message)
  }

  override def afterAll(): Unit = {
    assert("ok" === client.request[String, OkResponse](HttpMethods.POST, "auth/logout", "").message)
    HttpServerTest.unregister()
    super.afterAll()
  }
}
