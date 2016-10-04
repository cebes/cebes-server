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
 * Created by phvu on 07/09/16.
 */

package io.cebes.server.helpers

import akka.http.scaladsl.model.HttpMethods
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._

import io.cebes.server.models.CebesJsonProtocol._
import io.cebes.server.models.{OkResponse, UserLogin}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.concurrent.ExecutionContext.Implicits.global

trait HasClient extends FunSuite with BeforeAndAfterAll {

  val client = new Client

  /**
    * Before and after all
    */

  override def beforeAll(): Unit = {
    super.beforeAll()
    HttpServerTest.register()
    Client.register()
    assert("ok" === client.request[UserLogin, OkResponse](HttpMethods.POST,
      "auth/login", UserLogin("foo", "bar")).message)
  }

  override def afterAll(): Unit = {
    assert("ok" === client.request[String, OkResponse](HttpMethods.POST, "auth/logout", "").message)
    Client.unregister()
    HttpServerTest.unregister()
    super.afterAll()
  }
}
