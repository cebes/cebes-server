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

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import io.cebes.server.models._
import io.cebes.server.routes.AbstractRouteSuite
import io.cebes.server.routes.df.CebesDfProtocol._
import org.scalatest.BeforeAndAfterAll

class DataframeHandlerSuite extends AbstractRouteSuite with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    super.beforeAll()
    createOrReplaceCylinderBands()
  }

  test("sample") {
    val df = getCylinderBands

    val df2 = waitDf(postAsync[SampleRequest, DataframeResponse]("df/sample",
      SampleRequest(df.id, withReplacement = true, 0.5, 42)))
  }
}