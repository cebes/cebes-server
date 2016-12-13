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
import akka.http.scaladsl.model.{StatusCodes, headers => akkaHeaders}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import io.cebes.server.helpers.{ServerException, TestPropertyHelper}
import io.cebes.server.models._
import io.cebes.server.routes.Routes
import io.cebes.server.routes.df.CebesDfProtocol._
import io.cebes.server.util.Retries
import org.scalatest.FunSuite

import scala.util.{Failure, Success, Try}

class DataframeHandlerSuite extends FunSuite with TestPropertyHelper with ScalatestRouteTest with Routes {



  test("sample") {



    Post("/v1/df/sql", "SHOW TABLES").withHeaders(authHeaders) ~>
      routes ~> check {
      println(response)
      assert(responseAs[DataframeResponse].id.clockSequence() > 0)
    }

    //val result = client.requestAndWait[SampleRequest, DataframeResponse](HttpMethods.POST, "df/sample",
    //  SampleRequest()
    //  )
    //assert(result.isDefined && result.get.isInstanceOf[DataframeResponse])
  }

  //test("sql") {
  //val dfId = getCylinderBands
  //println(dfId)
  //}
}