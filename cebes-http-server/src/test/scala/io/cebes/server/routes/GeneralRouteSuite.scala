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
 * Created by phvu on 28/12/2016.
 */

package io.cebes.server.routes

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import io.cebes.http.server.HttpJsonProtocol._
import io.cebes.http.server.VersionResponse
import io.cebes.server.http.CebesHttpServer

class GeneralRouteSuite extends AbstractRouteSuite {

  test("version") {
    Get("/version") ~> serverRoutes ~> check {
      val v = responseAs[VersionResponse]
      assert(v.api === CebesHttpServer.API_VERSION)
    }
  }

  test("index") {
    Get("/") ~> serverRoutes ~> check {
      assert(handled)
    }
  }
}
