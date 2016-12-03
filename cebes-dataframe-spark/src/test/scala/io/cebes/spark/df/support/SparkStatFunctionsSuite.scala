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
 * Created by phvu on 03/12/2016.
 */

package io.cebes.spark.df.support

import io.cebes.common.CebesBackendException
import io.cebes.df.functions
import io.cebes.df.types.storage.LongType
import io.cebes.spark.helpers.{CebesBaseSuite, TestDataHelper, TestPropertyHelper}

class SparkStatFunctionsSuite extends CebesBaseSuite
  with TestPropertyHelper with TestDataHelper {

  override def beforeAll(): Unit = {
    super.beforeAll()
    createOrReplaceCylinderBands()
  }

  test("approxQuantile") {
    val df = getCylinderBands

    val quantiles = df.stat.approxQuantile("job_number", Array(0, 0.5, 1), 0.1)
    assert(quantiles.length === 3)

    intercept[IllegalArgumentException](df.stat.approxQuantile("customer", Array(0, 0.5, 1), 0.1))
  }

  test("cov") {
    val df = getCylinderBands

    assert(df.stat.cov("proof_cut", "viscosity") > 0)
    intercept[IllegalArgumentException](df.stat.cov("customer", "press"))
  }

  test("corr") {
    val df = getCylinderBands

    assert(df.stat.corr("proof_cut", "viscosity") > 0)
    intercept[IllegalArgumentException](df.stat.corr("customer", "press"))
  }

  test("crosstab") {
    val df = getCylinderBands

    val customerCount = df.select(functions.countDistinct("customer").as("count"))
      .take().get[Long]("count").map(_.head).get
    assert(customerCount > 0)
    assert(customerCount < df.numRows)
    val crosstab = df.stat.crosstab("customer", "blade_mfg")
    assert(crosstab.numRows === customerCount)
    assert(crosstab.numCols === 4)
    assert(crosstab.schema.tail.forall(_.storageType === LongType))

    intercept[CebesBackendException](df.stat.crosstab("non_existed_column", "blade_mfg"))
  }

  test("freqItems") {
    val df = getCylinderBands

    val df1 = df.stat.freqItems(Seq("customer", "grain_screened"))
    assert(df1.numCols === 2)
    assert(df1.numRows === 1)

    intercept[IllegalArgumentException](df.stat.freqItems(Seq("non_existed_column", "customer")))
  }

  test("sampleBy") {
    val df = getCylinderBands

    val df1 = df.stat.sampleBy("customer", Map("GUIDEPOSTS" -> 1, "ECKERD" -> 0.2, "TVGUIDE" -> 0.6), 42)
    assert(df1.numCols === df.numCols)
    assert(df1.numRows < df.numRows)
    assert(df1.select(functions.countDistinct("customer").as("cnt")).take().get[Long]("cnt").map(_.head).get === 3)
  }
}
