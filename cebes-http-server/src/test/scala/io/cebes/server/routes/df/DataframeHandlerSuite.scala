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
import io.cebes.df.sample.DataSample
import io.cebes.server.client.ServerException
import io.cebes.server.routes.{AbstractRouteSuite, DataframeResponse}
import io.cebes.server.routes.df.HttpDfJsonProtocol._
import org.scalatest.BeforeAndAfterAll

class DataframeHandlerSuite extends AbstractRouteSuite with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    super.beforeAll()
    createOrReplaceCylinderBands()
  }

  test("count") {
    val df = getCylinderBands

    val rows = wait[Long](postAsync[CountRequest, Long]("df/count", CountRequest(df.id)))
    assert(rows === 540)

    val ex = intercept[ServerException] {
      wait[Long](postAsync[CountRequest, Long]("df/count", CountRequest(UUID.randomUUID())))
    }
    assert(ex.message.startsWith("Dataframe ID not found"))
  }

  test("sample") {
    val df = getCylinderBands

    val df2 = waitDf(postAsync[SampleRequest, DataframeResponse]("df/sample",
      SampleRequest(df.id, withReplacement = true, 0.5, 42)))
    assert(df2.schema === df.schema)
    assert(df2.id !== df.id)

    val ex = intercept[ServerException] {
      waitDf(postAsync[SampleRequest, DataframeResponse]("df/sample",
        SampleRequest(UUID.randomUUID(), withReplacement = true, 0.5, 42)))
    }
    assert(ex.message.startsWith("Dataframe ID not found"))
  }

  test("take") {
    val df = getCylinderBands

    val sample = wait[DataSample](postAsync[TakeRequest, DataSample]("df/take", TakeRequest(df.id, 15)))
    assert(sample.schema === df.schema)
    assert(sample.data.length === sample.schema.size)
    assert(sample.data.forall(_.length === 15))

    val ex1 = intercept[ServerException] {
      wait[DataSample](postAsync[TakeRequest, DataSample]("df/take", TakeRequest(df.id, -15)))
    }
    assert(ex1.getMessage.startsWith("The limit expression must be equal to or greater than 0"))

    val ex2 = intercept[ServerException] {
      wait[DataSample](postAsync[TakeRequest, DataSample]("df/take", TakeRequest(UUID.randomUUID(), 15)))
    }
    assert(ex2.message.startsWith("Dataframe ID not found"))
  }

  test("drop") {
    val df = getCylinderBands

    val df1 = waitDf(postAsync[ColumnsRequest, DataframeResponse]("df/dropcolumns",
      ColumnsRequest(df.id, Array("TimeStamp", "ink_cOlor", "wrong_column"))))
    assert(df1.schema.size === df.schema.size - 2)
    assert(df1.schema.get("timestamp").isEmpty)
    assert(df1.schema.get("ink_cOlor").isEmpty)

    val df2 = waitDf(postAsync[ColumnsRequest, DataframeResponse]("df/dropcolumns",
      ColumnsRequest(df.id, Array())))
    assert(df2.schema.size === df.schema.size)
    assert(df2.schema.get("timestamp").nonEmpty)
    assert(df2.schema.get("ink_cOlor").nonEmpty)
    assert(df2.schema === df.schema)
  }

  test("dropDuplicates") {
    val df = getCylinderBands

    val df1 = waitDf(postAsync[ColumnsRequest, DataframeResponse]("df/dropduplicates",
      ColumnsRequest(df.id, Array("TimeStamp", "ink_cOlor"))))
    assert(df1.schema === df.schema)

    val ex1 = intercept[ServerException] {
      waitDf(postAsync[ColumnsRequest, DataframeResponse]("df/dropduplicates",
        ColumnsRequest(df.id, Array("TimeStamp", "ink_cOlor", "wrong_column"))))
    }
    assert(ex1.getMessage.startsWith("Spark query analysis exception: Cannot resolve column name \"wrong_column\""))

    val df2 = waitDf(postAsync[ColumnsRequest, DataframeResponse]("df/dropcolumns",
      ColumnsRequest(df.id, Array())))
    assert(df2.schema === df.schema)
  }
}