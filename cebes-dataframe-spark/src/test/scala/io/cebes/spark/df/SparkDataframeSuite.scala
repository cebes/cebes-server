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
 * Created by phvu on 06/10/16.
 */

package io.cebes.spark.df

import io.cebes.spark.helpers.{TestDataHelper, TestPropertyHelper}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class SparkDataframeSuite extends FunSuite with BeforeAndAfterAll
  with TestPropertyHelper with TestDataHelper {

  override def beforeAll(): Unit = {
    super.beforeAll()
    createOrReplaceCylinderBands()
  }

  test("Type conversions in take()") {
    val df = sparkDataframeService.sql("SELECT customer, " +
      " customer IN ('TVGUIDE', 'MASSEY') AS my_customer_bool, " +
      "CAST(unit_number AS BYTE) AS unit_number_byte, " +
      "CAST(proof_cut AS SHORT) AS proof_cut_short, " +
      "proof_cut AS proof_cut_int, " +
      "CAST(proof_cut AS LONG) AS proof_cut_long, " +
      "CAST(roughness AS FLOAT) AS roughness_float, " +
      "CAST(roughness AS DOUBLE) as roughness_double, " +
      "IF(roughness > 0.6, NULL, roughness) as roughness_double_null, " +
      "ARRAY(CAST(proof_cut AS DOUBLE), CAST(viscosity AS DOUBLE), CAST(caliper AS DOUBLE)) AS arr_double, " +
      "UNHEX(HEX(customer)) AS customer_unhex_binary, " +
      "CURRENT_DATE(), " +
      "CURRENT_TIMESTAMP() " +
      "FROM cylinder_bands LIMIT 10")
    assert(df.numCols === 13)

    val sample = df.take(10)
    assert(sample.numCols === 13)
    sample.columns.foreach { c =>
      assert(c.length === 10)
      assert(!c.forall(_ === null))
    }
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
