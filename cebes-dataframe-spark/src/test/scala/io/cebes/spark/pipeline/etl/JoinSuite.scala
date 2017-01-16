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
package io.cebes.spark.pipeline.etl

import io.cebes.df.functions
import io.cebes.pipeline.models.DataframeMessage
import io.cebes.spark.helpers.{CebesBaseSuite, TestDataHelper, TestPipelineHelper}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class JoinSuite extends CebesBaseSuite with TestDataHelper with TestPipelineHelper {

  override def beforeAll(): Unit = {
    super.beforeAll()
    createOrReplaceCylinderBands()
  }

  test("join") {
    val df = getCylinderBands

    val df1 = df.select("*").where(df("customer").isin("GUIDEPOSTS", "ECKERD")).alias("small")
    val df2 = df.select("*").where(df("customer").isin("GUIDEPOSTS", "ECKERD", "TARGET")).alias("big")

    // inner join
    val s = Join().setName("join")
    s.set(s.joinType, "inner")
    s.set(s.joinExprs, functions.col("small.customer") === functions.col("big.customer"))
    s.input(0, Future(DataframeMessage(df1)))

    val ex0 = intercept[NoSuchElementException] {
      resultDf(s.output(0))
    }
    assert(ex0.getMessage.contains("Slot named right is not specified"))

    s.input(1, Future(DataframeMessage(df2)))
    val dfj1 = resultDf(s.output(0))
    assert(dfj1.numRows === 117)

    // invalid param
    val ex = intercept[IllegalArgumentException] {
      s.set(s.joinType, "wrong_type")
    }
    assert(ex.getMessage.contains("Invalid value (wrong_type) for parameter joinType"))
  }
}
