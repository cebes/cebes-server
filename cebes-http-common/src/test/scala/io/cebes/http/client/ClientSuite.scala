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
package io.cebes.http.client

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpMethods
import akka.stream.ActorMaterializer
import org.scalatest.FunSuite

import scala.concurrent.ExecutionContext.Implicits.global

class ClientSuite extends FunSuite {

  implicit val system: ActorSystem = ActorSystem("ClientSuite")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  test("request to google") {
    val client = new Client("www.google.com", 80)
    val result = client.requestSync[String, String](HttpMethods.GET, "/about/", "")
    assert(result.length > 0)

    // failed POST
    val ex = intercept[ServerException] {
      client.requestSync[String, String](HttpMethods.POST, "/about/", "")
    }
    assert(ex.message.length > 0)
  }
}
