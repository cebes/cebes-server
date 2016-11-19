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
import io.cebes.df.functions
import io.cebes.df.types.storage.BooleanType
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
    assert(df3.numRows === df1.numRows)
    assert(df3.numCols === 1)
    assert(df3.schema(df3.columns.head).storageType === BooleanType)
  }

  test("NotEqual") {
    val df1 = getCylinderBands

    val df2 = df1.select((df1("proof_on_ctd_ink") =!= df1("grain_screened")).as("compare"),
      df1("proof_on_ctd_ink"), df1("grain_screened"))
    val sample2 = df2.take(100)
    assert((sample2.get[String]("proof_on_ctd_ink").get,
      sample2.get[String]("grain_screened").get,
      sample2.get[Boolean]("compare").get).zipped.forall {
      (s1, s2, b) => b match {
        case true => s1 !== s2
        case false => s1 === s2
      }
    })

    val df3 = df1.select(df1("timestamp").notEqual(df1("grain_screened")))
    assert(df3.numRows === df1.numRows)
    assert(df3.numCols === 1)
    assert(df3.schema(df3.columns.head).storageType === BooleanType)
  }

  test("Greater") {
    val df1 = getCylinderBands

    val df2 = df1.select((df1("caliper") > df1("roughness")).as("compare"),
      df1("caliper"), df1("roughness"))
    val sample2 = df2.take(100)
    assert((sample2.get[Float]("caliper").get,
      sample2.get[Float]("roughness").get,
      sample2.get[Boolean]("compare").get).zipped.forall {
      (v1, v2, b) =>
        b match {
          case true => v1 > v2
          case false =>
            // this happens when one of the two arguments are null
            // which will be translated to 0.0 in Float
            v1 <= v2 || v1 === 0.0 || v2 === 0.0
        }
    })

    // different data types still work
    val df3 = df1.select(df1("timestamp").gt(df1("cylinder_number")))
    assert(df3.numRows === df1.numRows)
    assert(df3.numCols === 1)
    assert(df3.schema(df3.columns.head).storageType === BooleanType)
  }

  test("LessThan") {
    val df1 = getCylinderBands

    val df2 = df1.select((df1("caliper") < df1("roughness")).as("compare"),
      df1("caliper"), df1("roughness"))
    val sample2 = df2.take(100)
    assert((sample2.get[Float]("caliper").get,
      sample2.get[Float]("roughness").get,
      sample2.get[Boolean]("compare").get).zipped.forall {
      (v1, v2, b) =>
        b match {
          case true => v1 < v2
          case false =>
            // this happens when one of the two arguments are null
            // which will be translated to 0.0 in Float
            v1 >= v2 || v1 === 0.0 || v2 === 0.0
        }
    })

    // different data types still work
    val df3 = df1.select(df1("timestamp").lt(df1("cylinder_number")))
    assert(df3.numRows === df1.numRows)
    assert(df3.numCols === 1)
    assert(df3.schema(df3.columns.head).storageType === BooleanType)
  }

  test("GreaterOrEqual") {
    val df1 = getCylinderBands

    val df2 = df1.select((df1("caliper") >= df1("roughness")).as("compare"),
      df1("caliper"), df1("roughness"))
    val sample2 = df2.take(100)
    assert((sample2.get[Float]("caliper").get,
      sample2.get[Float]("roughness").get,
      sample2.get[Boolean]("compare").get).zipped.forall {
      (v1, v2, b) =>
        b match {
          case true => v1 >= v2
          case false =>
            // this happens when one of the two arguments are null
            // which will be translated to 0.0 in Float
            v1 < v2 || v1 === 0.0 || v2 === 0.0
        }
    })

    // different data types still work
    val df3 = df1.select(df1("timestamp").geq(df1("cylinder_number")))
    assert(df3.numRows === df1.numRows)
    assert(df3.numCols === 1)
    assert(df3.schema(df3.columns.head).storageType === BooleanType)
  }

  test("LessThanOrEqual") {
    val df1 = getCylinderBands

    val df2 = df1.select((df1("caliper") <= df1("roughness")).as("compare"),
      df1("caliper"), df1("roughness"))
    val sample2 = df2.take(100)
    assert((sample2.get[Float]("caliper").get,
      sample2.get[Float]("roughness").get,
      sample2.get[Boolean]("compare").get).zipped.forall {
      (v1, v2, b) =>
        b match {
          case true => v1 <= v2
          case false =>
            // this happens when one of the two arguments are null
            // which will be translated to 0.0 in Float
            v1 > v2 || v1 === 0.0 || v2 === 0.0
        }
    })

    // different data types still work
    val df3 = df1.select(df1("timestamp").leq(df1("cylinder_number")))
    assert(df3.numRows === df1.numRows)
    assert(df3.numCols === 1)
    assert(df3.schema(df3.columns.head).storageType === BooleanType)
  }

  test("eqNullSafe") {
    val df1 = getCylinderBands

    val df2 = df1.select((df1("caliper") <=> df1("roughness")).as("compare"),
      df1("caliper"), df1("roughness"))
    assert(df2.numRows === df1.numRows)
    assert(df2.numCols === 3)
    assert(df2.schema("compare").storageType === BooleanType)

    // different data types still work
    val df3 = df1.select(df1("timestamp").eqNullSafe(df1("cylinder_number")))
    assert(df3.numRows === df1.numRows)
    assert(df3.numCols === 1)
    assert(df3.schema(df3.columns.head).storageType === BooleanType)
  }

  test("CaseWhen") {
    val df1 = getCylinderBands

    val df2 = df1.select(functions.when(df1("paper_mill_location") === "NorthUS", 1).
      when(df1("paper_mill_location") === "CANADIAN", 2).otherwise(3).as("case_col"),
      df1("paper_mill_location"))
    val sample2 = df2.take(100)
    assert((sample2.get[String]("paper_mill_location").get,
      sample2.get[Int]("case_col").get).zipped.forall {
      (s, v) => v match {
        case 1 => s === "NorthUS"
        case 2 => s === "CANADIAN"
        case 3 => !Seq("NorthUS", "CANADIAN").contains(s)
      }
    })

    val df3 = df1.select(functions.when(df1("paper_mill_location") === "NorthUS", 1).
      when(df1("paper_mill_location") === "CANADIAN", 2).as("case_col"),
      df1("paper_mill_location"))
    val sample3 = df3.take(100)
    assert((sample3.get[String]("paper_mill_location").get,
      sample3.get[Any]("case_col").get).zipped.forall {
      (s, v) => v match {
        case 1 => s === "NorthUS"
        case 2 => s === "CANADIAN"
        case null => !Seq("NorthUS", "CANADIAN").contains(s)
      }
    })

    val ex1 = intercept[IllegalArgumentException] {
      df1.select(df1("timestamp").when(df1("cylinder_number") === "AAA", 100))
    }
    assert(ex1.getMessage.contains("when() can only be applied on a Column previously generated by when() function"))

    val ex2 = intercept[IllegalArgumentException] {
      df1.select(functions.when(df1("cylinder_number") === "AAA", 100).otherwise(200).
        when(df1("cylinder_number") === "BBB", 100))
    }
    assert(ex2.getMessage.contains("when() cannot be applied once otherwise() is applied"))

    val ex3 = intercept[IllegalArgumentException] {
      df1.select(functions.when(df1("cylinder_number") === "AAA", 100).otherwise(200).otherwise(300))
    }
    assert(ex3.getMessage.contains("otherwise() can only be applied once on a Column previously generated by when()"))

    val ex4 = intercept[IllegalArgumentException] {
      df1.select(df1("timestamp").otherwise(df1("cylinder_number") === "AAA"))
    }
    assert(ex4.getMessage.contains("otherwise() can only be applied on a Column previously generated by when()"))
  }

  test("between") {
    val df = getCylinderBands

    val df1 = df.sort(df("timestamp")).select(
      functions.col("timestamp").between(19900414, 19900505).as("ts_bool"),
      functions.col("timestamp").as("ts"))
    val sample1 = df1.take(100)
    assert((sample1.get[Boolean]("ts_bool").get,
      sample1.get[Long]("ts").get).zipped.forall {
      (b, v) =>
        b === (19900414 <= v && v <= 19900505)
    })

    // between 2 columns
    val df2 = df.select(
      df("viscosity").between(df("proof_cut"), df("plating_tank")).as("btw"),
      df("viscosity"), df("proof_cut"), df("plating_tank")).limit(100)
    assert(df2.numCols === 4)
    assert(df2.numRows === 100)
  }
}
