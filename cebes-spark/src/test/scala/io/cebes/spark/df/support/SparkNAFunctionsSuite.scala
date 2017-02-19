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
import io.cebes.spark.helpers.{TestDataHelper, TestPropertyHelper}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class SparkNAFunctionsSuite extends FunSuite with BeforeAndAfterAll
  with TestPropertyHelper with TestDataHelper {

  override def beforeAll(): Unit = {
    super.beforeAll()
    createOrReplaceCylinderBands()
  }

  test("drop") {
    val df = getCylinderBands
    assert(df.numRows === 540)

    // any, on all columns
    val df1 = df.na.drop()
    assert(df1.columns === df.columns)
    assert(df1.numRows === 357)

    // all, on all columns
    val df2 = df.na.drop(how = "all")
    assert(df2.numRows === df.numRows)

    intercept[IllegalArgumentException](df.na.drop(how = "unrecognized"))

    // any, on some columns
    val df3 = df.na.drop(Seq("plating_tank", "grain_screened"))
    assert(df3.numRows === 522)

    intercept[CebesBackendException](df.na.drop(Seq("non_existed_column")))

    // all, on some columns
    val df4 = df.na.drop("all", Seq("customer", "grain_screened"))
    assert(df4.numRows === df.numRows)

    val df5 = df.na.drop("all", Seq("plating_tank", "grain_screened"))
    assert(df5.numRows === df.numRows)

    val df6 = df.na.drop("all", Seq("plating_tank", "proof_cut"))
    assert(df6.numRows === 535)

    // minNonNull = 2
    val df7 = df.na.drop(2)
    assert(df7.numRows === df.numRows)

    // minNonNull on some columns
    val df8 = df.na.drop(2, Seq("grain_screened", "plating_tank", "proof_cut"))
    assert(df8.numRows === 535)
  }

  test("fill") {
    val df = getCylinderBands

    val df1 = df.na.fill(0.2)
    assert(df1.na.drop().numRows === df.numRows)

    val df2 = df.na.fill(0.2, Seq("blade_pressure", "plating_tank"))
    assert(df2.na.drop(Seq("blade_pressure", "plating_tank")).numRows === df.numRows)

    val df3 = df.na.fill("N/A")
    assert(df3.na.drop().numRows === 357)

    val df4 = df.na.fill("N/A", Seq("paper_mill_location"))
    assert(df4.na.drop(Seq("paper_mill_location")).numRows === df.numRows)

    val df5 = df.na.fill(Map("blade_pressure" -> 2.0, "paper_mill_location" -> "N/A"))
    assert(df5.na.drop(Seq("blade_pressure", "paper_mill_location")).numRows === df.numRows)

    intercept[CebesBackendException](df.na.fill(Map("blade_pressure" -> 2.0, "non_existed_column" -> "N/A")))
  }

  test("replace") {
    val df = getCylinderBands

    val nRowCustomer = df.where(df("customer") === "GUIDEPOSTS").numRows
    assert(nRowCustomer > 0)
    val df1 = df.na.replace("customer", Map("GUIDEPOSTS" -> "GUIDEPOSTS-replaced"))
    assert(df1.where(df1("customer") === "GUIDEPOSTS").numRows === 0)
    assert(df1.where(df1("customer") === "GUIDEPOSTS-replaced").numRows === nRowCustomer)

    val nRowJobNumber = df.where(df("job_number") === 47103).numRows
    assert(nRowJobNumber > 0)
    val df2 = df.na.replace(Seq("unit_number", "job_number"), Map(47103 -> 42))
    assert(df2.where(df2("unit_number") === 47103).numRows === 0)
    assert(df2.where(df2("job_number") === 47103).numRows === 0)
    assert(df2.where(df2("job_number") === 42).numRows === nRowJobNumber)

    // elements in valueMaps must have the same data type
    intercept[IllegalArgumentException] {
      df.na.replace(Seq("unit_number", "job_number"), Map(47103 -> 42, "GUIDEPOSTS" -> "GUIDEPOSTS-replaced"))
    }

    // replace a value with null
    val df4 = df.na.replace(Seq("unit_number", "customer"), Map("bbbb" -> "aaaa", "GUIDEPOSTS" -> null))
    assert(df4.where(df4("customer") === "GUIDEPOSTS").numRows === 0)
    assert(df4.where(df4("customer").isNull).numRows === nRowCustomer)

    // no exception, even non-existed column
    val df3 = df.na.replace("non_existed_column", Map(100 -> 200))
    assert(df3.id !== df.id)
    assert(df3.columns === df.columns)
    assert(df3.numRows === df.numRows)
  }
}
