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
 * Created by phvu on 13/12/2016.
 */

package io.cebes.server.routes.df

import java.util.UUID

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import io.cebes.server.models._
import io.cebes.server.routes.AbstractRouteSuite
import io.cebes.server.routes.df.CebesDfProtocol._

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

class DataframeHandlerSuite extends AbstractRouteSuite {

  test("sql") {
    val fu = postAndWait("df/sql", "SHOW TABLES")
    fu.onComplete {
      case Success(r) =>
        println(r)
        assert(r.response.get.convertTo[DataframeResponse].id.clockSequence() > 0)
      case Failure(f) =>
        println(f)
    }
    Await.result(fu, Duration.Inf)
  }

  test("sample") {
    postAndWait("df/sample", SampleRequest(UUID.randomUUID(), withReplacement = true, 0.5, 42)).onComplete {
      case Success(r) =>
        println(r)
        assert(r.response.get.convertTo[DataframeResponse].id.clockSequence() > 0)
      case Failure(f) =>
        println(f)
    }
  }
}