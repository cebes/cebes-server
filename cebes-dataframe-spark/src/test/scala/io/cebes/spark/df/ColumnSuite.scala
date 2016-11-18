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
 * Created by phvu on 18/11/2016.
 */

package io.cebes.spark.df

import io.cebes.common.CebesBackendException
import io.cebes.spark.helpers.TestDataHelper
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class ColumnSuite extends FunSuite with TestDataHelper with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    super.beforeAll()
    createOrReplaceCylinderBands()
  }

  test("sortOrder") {
    // very light because sort was already tested in SparkDataframeSuite
    val df = getCylinderBands

    // mixed
    val df4 = df.sort(df.col("timestamp").desc, df.col("cylinder_number").asc)
    val sample4 = df4.take(10)
    val col4Ts = sample4.get[Long]("Timestamp").get
    val col4Cn = sample4.get[String]("cylinder_number").get
    assert(col4Ts.head === col4Ts(1))
    assert(col4Cn.head < col4Cn(1))
    assert(col4Ts.head > col4Ts.last)
  }

  test("UnaryMinus") {
    val df = getCylinderBands

    val df1 = df.select((-df("timestamp")).as("minus_timestamp"), df("timestamp"))
    val sample1 = df1.take(100)
    assert(df1.numCols === 2)
    assert(df1.schema.contains("minus_timestamp"))
    assert(sample1.get[Long]("timestamp").get === sample1.get[Long]("minus_timestamp").get.map(v => -v))

    // double negation
    val df2 = df.select((-(-df("timestamp"))).as("still_timestamp"), df("timestamp"))
    val sample2 = df2.take(100)
    assert(df2.numCols === 2)
    assert(df2.schema.contains("still_timestamp"))
    assert(sample2.get[Long]("timestamp").get === sample2.get[Long]("still_timestamp").get)

    // cannot negate a string column
    intercept[CebesBackendException] {
      df.select(-df("cylinder_number"))
    }
  }

  test("Not") {
    val df1 = sparkDataframeService.sql(
      "SELECT CAST(grain_screened AS BOOLEAN) AS grain_screened_bool, " +
        s"grain_screened FROM $cylinderBandsTableName")

    val df2 = df1.select((!df1("grain_screened_bool")).as("grain_screened_not"),
      df1("grain_screened_bool"), df1("grain_screened"))
    val sample2 = df2.take(100)
    assert(df2.columns === Seq("grain_screened_not", "grain_screened_bool", "grain_screened"))
    assert((sample2.get[Boolean]("grain_screened_bool").get,
      sample2.get[Boolean]("grain_screened_not").get,
      sample2.get[String]("grain_screened").get).zipped.forall {
      (b, bNot, s) =>
        s match {
          case "" => !b && !bNot
          case "YES" => b && !bNot
          case "NO" => !b && bNot
        }
    })

    // cannot apply Not on string column
    intercept[CebesBackendException] {
      df1.select(!df1("grain_screened"))
    }
  }

  test("equalTo") {
    val df1 = getCylinderBands

    val df2 = df1.select((df1("proof_on_ctd_ink") === df1("grain_screened")).as("compare"),
      df1("proof_on_ctd_ink"), df1("grain_screened"))
    val sample2 = df2.take(100)
    assert((sample2.get[String]("proof_on_ctd_ink").get,
      sample2.get[String]("grain_screened").get,
      sample2.get[Boolean]("compare").get).zipped.forall {
      (s1, s2, b) => b match {
        case true => s1 === s2
        case false => s1 !== s2
      }
    })

    val df3 = df1.select(df1("timestamp").equalTo(df1("grain_screened")))
  }

}
