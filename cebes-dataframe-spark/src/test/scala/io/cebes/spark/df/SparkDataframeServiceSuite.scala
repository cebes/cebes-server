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
 * Created by phvu on 29/09/16.
 */

package io.cebes.spark.df

import io.cebes.spark.helpers.{TestPropertyHelper, TestDataHelper}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class SparkDataframeServiceSuite extends FunSuite with BeforeAndAfterAll
  with TestPropertyHelper with TestDataHelper {

  override def beforeAll(): Unit = {
    super.beforeAll()
    createOrReplaceCylinderBands()
  }

  test("Simple SQL") {
    val df = sparkDataframeService.sql("SELECT * FROM cylinder_bands")
    assert(df.numCols === 40)
    assert(df.numRows === 540)
    val sample = df.take(10)
    assert(sample.numCols === df.numCols)
  }

  test("Type conversions in take()") {
    //val df = sparkDataframeService.sql("SELECT customer, " +
    //  " FROM cylinder_bands")
    // TODO: implement this
  }

  test("Dataframe Sample") {
    val df = sparkDataframeService.sql("SELECT * FROM cylinder_bands")
    assert(df.numCols === 40)
    assert(df.numRows === 540)

    val df2 = df.sample(withReplacement = false, 0.1, 42)
    assert(df2.numCols === df.numCols)
    assert(df2.numRows > 0)

    val df3 = df.sample(withReplacement = true, 2.0, 42)
    assert(df3.numCols === df.numCols)
    assert(df3.numRows > df.numRows)
  }
}
