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
import io.cebes.df.types.storage._
import io.cebes.spark.helpers.{CebesBaseSuite, TestDataHelper}


class ColumnSuite extends CebesBaseSuite with TestDataHelper {

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
      (s1, s2, b) => b === (s1 === s2)
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
      (s1, s2, b) => b === (s1 !== s2)
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
        if (b) {
          v1 > v2
        } else {
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
        if (b) {
          v1 < v2
        } else {
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
        if (b) {
          v1 >= v2
        } else {
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
        if (b) {
          v1 <= v2
        } else {
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

  test("isNaN") {
    val df = getCylinderBands

    // on float column
    val df1 = df.select(df("proof_cut").isNaN.as("proof_cut_bool"))
    assert(df1.numCols === 1)
    assert(df1.numRows === df.numRows)
    assert(df1.schema("proof_cut_bool").storageType === BooleanType)

    // on string column
    val df2 = df.select(df("cylinder_number").isNaN.as("cylinder_number_bool"))
    assert(df2.numCols === 1)
    assert(df2.numRows === df.numRows)
    assert(df2.schema("cylinder_number_bool").storageType === BooleanType)
  }

  test("isNull") {
    val df = getCylinderBands

    val df1 = df.select(df("plating_tank").isNull.as("plating_tank_null"),
      df("plating_tank"))
    val sample1 = df1.take(100)
    assert(df1.numCols === 2)
    assert(df1.numRows === df.numRows)
    assert(df1.schema(df1.columns.head).storageType === BooleanType)
    assert(sample1.rows.forall {
      case Seq(b: Boolean, null) => b
      case Seq(b: Boolean, _: Int) => !b
    })
  }

  test("isNotNull") {
    val df = getCylinderBands

    val df1 = df.select(df("plating_tank").isNotNull.as("plating_tank_not_null"),
      df("plating_tank"))
    val sample1 = df1.take(100)
    assert(df1.numCols === 2)
    assert(df1.numRows === df.numRows)
    assert(df1.schema(df1.columns.head).storageType === BooleanType)
    assert(sample1.rows.forall {
      case Seq(b: Boolean, null) => !b
      case Seq(b: Boolean, _: Int) => b
    })
  }

  test("or") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select((df("grain_screened") === "YES" || df("proof_on_ctd_ink") === "YES").as("or_col"),
      df("grain_screened"),	df("proof_on_ctd_ink"))
    val sample1 = df1.take(100)
    assert(df1.numCols === 3)
    assert(df1.numRows === 100)
    assert(df1.columns === Seq("or_col", "grain_screened", "proof_on_ctd_ink"))
    assert(df1.schema("or_col").storageType === BooleanType)
    assert(sample1.rows.forall {
      case Seq(b: Boolean, s1: String, s2: String) => b === (s1 === "YES" || s2 === "YES")
    })

    val df2 = df.select((df("job_number") > 20000).or(df("unit_number") <= 10).as("or_col"),
      df("job_number"),	df("unit_number"))
    val sample2 = df2.take(100)
    assert(df2.numCols === 3)
    assert(df2.numRows === 100)
    assert(df2.columns === Seq("or_col", "job_number", "unit_number"))
    assert(df2.schema("or_col").storageType === BooleanType)
    assert(sample2.rows.forall {
      case Seq(b: Boolean, v1: Int, v2: Int) => b === (v1 > 20000 || v2 <= 10)
    })
  }

  test("and") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select((df("grain_screened") === "YES" && df("proof_on_ctd_ink") === "YES").as("and_col"),
      df("grain_screened"),	df("proof_on_ctd_ink"))
    val sample1 = df1.take(100)
    assert(df1.numCols === 3)
    assert(df1.numRows === 100)
    assert(df1.columns === Seq("and_col", "grain_screened", "proof_on_ctd_ink"))
    assert(df1.schema("and_col").storageType === BooleanType)
    assert(sample1.rows.forall {
      case Seq(b: Boolean, s1: String, s2: String) => b === (s1 === "YES" && s2 === "YES")
    })

    val df2 = df.select((df("job_number") > 20000).and(df("unit_number") <= 10).as("and_col"),
      df("job_number"),	df("unit_number"))
    val sample2 = df2.take(100)
    assert(df2.numCols === 3)
    assert(df2.numRows === 100)
    assert(df2.columns === Seq("and_col", "job_number", "unit_number"))
    assert(df2.schema("and_col").storageType === BooleanType)
    assert(sample2.rows.forall {
      case Seq(b: Boolean, v1: Int, v2: Int) => b === (v1 > 20000 && v2 <= 10)
    })
  }

  test("plus") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("job_number").plus(df("unit_number") + 15).as("plus_col"),
      df("job_number"),	df("unit_number"))
    val sample1 = df1.take(100)
    assert(df1.numCols === 3)
    assert(df1.numRows === 100)
    assert(df1.columns === Seq("plus_col", "job_number", "unit_number"))
    assert(df1.schema("plus_col").storageType === IntegerType)
    assert(sample1.rows.forall {
      case Seq(v: Int, v1: Int, v2: Int) => v === (v1 + v2 + 15)
    })

    // promoted to Double
    val df2 = df.select(df("job_number").plus(df("unit_number") + 19.4).as("plus_col"),
      df("job_number"),	df("unit_number"))
    val sample2 = df2.take(100)
    assert(df2.numCols === 3)
    assert(df2.numRows === 100)
    assert(df2.columns === Seq("plus_col", "job_number", "unit_number"))
    assert(df2.schema("plus_col").storageType === DoubleType)
    assert(sample2.rows.forall {
      case Seq(v: Double, v1: Int, v2: Int) => v === (v1 + v2 + 19.4)
    })

    // evaluated to null when applied on string column
    val df3 = df.select((df("grain_screened") + df("proof_on_ctd_ink")).as("plus_col"),
      df("grain_screened"),	df("proof_on_ctd_ink"))
    val sample3 = df3.take(100)
    assert(df3.numCols === 3)
    assert(df3.numRows === 100)
    assert(df3.columns === Seq("plus_col", "grain_screened", "proof_on_ctd_ink"))
    assert(df3.schema("plus_col").storageType === DoubleType)
    assert(sample3.get[Any]("plus_col").get.forall(Option(_).isEmpty))
  }

  test("minus") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("job_number").minus(df("unit_number") - 15).as("minus_col"),
      df("job_number"),	df("unit_number"))
    val sample1 = df1.take(100)
    assert(df1.numCols === 3)
    assert(df1.numRows === 100)
    assert(df1.columns === Seq("minus_col", "job_number", "unit_number"))
    assert(df1.schema("minus_col").storageType === IntegerType)
    assert(sample1.rows.forall {
      case Seq(v: Int, v1: Int, v2: Int) =>
        v === (v1 - (v2 - 15))
    })

    // promoted to Double
    val df2 = df.select(df("job_number").minus(df("unit_number") - 19.4).as("minus_col"),
      df("job_number"),	df("unit_number"))
    val sample2 = df2.take(100)
    assert(df2.numCols === 3)
    assert(df2.numRows === 100)
    assert(df2.columns === Seq("minus_col", "job_number", "unit_number"))
    assert(df2.schema("minus_col").storageType === DoubleType)
    assert(sample2.rows.forall {
      case Seq(v: Double, v1: Int, v2: Int) => v === (v1 - (v2 - 19.4))
    })

    // evaluated to null when applied on string column
    val df3 = df.select((df("grain_screened") - df("proof_on_ctd_ink")).as("minus_col"),
      df("grain_screened"),	df("proof_on_ctd_ink"))
    val sample3 = df3.take(100)
    assert(df3.numCols === 3)
    assert(df3.numRows === 100)
    assert(df3.columns === Seq("minus_col", "grain_screened", "proof_on_ctd_ink"))
    assert(df3.schema("minus_col").storageType === DoubleType)
    assert(sample3.get[Any]("minus_col").get.forall(Option(_).isEmpty))
  }

  test("multiply") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("job_number").multiply(df("unit_number") * 2).as("multiply_col"),
      df("job_number"),	df("unit_number"))
    val sample1 = df1.take(100)
    assert(df1.numCols === 3)
    assert(df1.numRows === 100)
    assert(df1.columns === Seq("multiply_col", "job_number", "unit_number"))
    assert(df1.schema("multiply_col").storageType === IntegerType)
    assert(sample1.rows.forall {
      case Seq(v: Int, v1: Int, v2: Int) => v === (v1 * v2 * 2)
    })

    // promoted to Double
    val df2 = df.select(df("job_number").multiply(df("unit_number") * 0.4).as("multiply_col"),
      df("job_number"),	df("unit_number"))
    val sample2 = df2.take(100)
    assert(df2.numCols === 3)
    assert(df2.numRows === 100)
    assert(df2.columns === Seq("multiply_col", "job_number", "unit_number"))
    assert(df2.schema("multiply_col").storageType === DoubleType)
    assert(sample2.rows.forall {
      case Seq(v: Double, v1: Int, v2: Int) =>
        v === (v1 * (v2 * 0.4))
    })

    // evaluated to null when applied on string column
    val df3 = df.select((df("grain_screened") * df("proof_on_ctd_ink")).as("multiply_col"),
      df("grain_screened"),	df("proof_on_ctd_ink"))
    val sample3 = df3.take(100)
    assert(df3.numCols === 3)
    assert(df3.numRows === 100)
    assert(df3.columns === Seq("multiply_col", "grain_screened", "proof_on_ctd_ink"))
    assert(df3.schema("multiply_col").storageType === DoubleType)
    assert(sample3.get[Any]("multiply_col").get.forall(Option(_).isEmpty))
  }

  test("divide") {
    val df = getCylinderBands.limit(100)

    // promoted to Double anyway
    val df1 = df.select(df("job_number").divide(df("unit_number") / 2).as("divide_col"),
      df("job_number"),	df("unit_number"))
    val sample1 = df1.take(100)
    assert(df1.numCols === 3)
    assert(df1.numRows === 100)
    assert(df1.columns === Seq("divide_col", "job_number", "unit_number"))
    assert(df1.schema("divide_col").storageType === DoubleType)
    assert(sample1.rows.forall {
      case Seq(v: Double, v1: Int, v2: Int) =>
        math.abs(v - (v1 / (v2 / 2.0))) <= 10E-10
    })

    // promoted to Double
    val df2 = df.select(df("job_number").divide(df("unit_number") / 2.5).as("divide_col"),
      df("job_number"),	df("unit_number"))
    val sample2 = df2.take(100)
    assert(df2.numCols === 3)
    assert(df2.numRows === 100)
    assert(df2.columns === Seq("divide_col", "job_number", "unit_number"))
    assert(df2.schema("divide_col").storageType === DoubleType)
    assert(sample2.rows.forall {
      case Seq(v: Double, v1: Int, v2: Int) => math.abs(v - (v1 / (v2 / 2.5))) <= 10E-10
    })

    // evaluated to null when applied on string column
    val df3 = df.select((df("grain_screened") / df("proof_on_ctd_ink")).as("divide_col"),
      df("grain_screened"),	df("proof_on_ctd_ink"))
    val sample3 = df3.take(100)
    assert(df3.numCols === 3)
    assert(df3.numRows === 100)
    assert(df3.columns === Seq("divide_col", "grain_screened", "proof_on_ctd_ink"))
    assert(df3.schema("divide_col").storageType === DoubleType)
    assert(sample3.get[Any]("divide_col").get.forall(Option(_).isEmpty))
  }

  test("modulo") {
    val df = getCylinderBands.limit(100)

    // modulo to another column
    val df1 = df.select(df("job_number").mod(df("unit_number")).as("mod_col"),
      df("job_number"),	df("unit_number"))
    val sample1 = df1.take(100)
    assert(df1.numCols === 3)
    assert(df1.numRows === 100)
    assert(df1.columns === Seq("mod_col", "job_number", "unit_number"))
    assert(df1.schema("mod_col").storageType === IntegerType)
    assert(sample1.rows.forall {
      case Seq(v: Int, v1: Int, v2: Int) => v === (v1 % v2)
    })

    // modulo to a number
    val df2 = df.select((df("job_number") % 3).as("mod_col"), df("job_number"))
    val sample2 = df2.take(100)
    assert(df2.numCols === 2)
    assert(df2.numRows === 100)
    assert(df2.columns === Seq("mod_col", "job_number"))
    assert(df2.schema("mod_col").storageType === IntegerType)
    assert(sample2.rows.forall {
      case Seq(v: Int, v1: Int) => v === (v1 % 3)
    })

    // modulo of a string column gives NULL
    val df3 = df.select((df("grain_screened") % 3).as("mod_col"), df("grain_screened"))
    val sample3 = df3.take(100)
    assert(df3.numCols === 2)
    assert(df3.numRows === 100)
    assert(df3.columns === Seq("mod_col", "grain_screened"))
    assert(df3.schema("mod_col").storageType === DoubleType)
    assert(sample3("mod_col").forall { _ === null })
  }

  test("isIn") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("grain_screened").isin(df("proof_on_ctd_ink"), "NO", 125).as("isin_col"),
      df("grain_screened"), df("proof_on_ctd_ink"))
    val sample1 = df1.take(100)
    assert(df1.columns === Seq("isin_col", "grain_screened", "proof_on_ctd_ink"))
    assert(df1.schema("isin_col").storageType === BooleanType)
    assert(sample1.rows.forall {
      case Seq(v: Boolean, s1: String, s2: String) => v === Seq(s2, "NO").contains(s1)
    })

    // empty list: false all the time
    val df2 = df.select(df("grain_screened").isin().as("isin_col"), df("grain_screened"))
    val sample2 = df2.take(100)
    assert(df2.columns === Seq("isin_col", "grain_screened"))
    assert(df2.schema("isin_col").storageType === BooleanType)
    assert(sample2.get[Boolean]("isin_col").get.forall(b => !b))
  }

  test("like") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("customer").like("%M_T").as("like_col"), df("customer"))
    val sample1 = df1.take(100)
    assert(df1.columns === Seq("like_col", "customer"))
    assert(df1.schema("like_col").storageType === BooleanType)
    assert(sample1.rows.forall {
      case Seq(s: Boolean, c: String) => s === (c === "MODMAT")
    })

    // use RLIKE expression: false all the time
    val df2 = df.select(df("customer").like("^MOD[M|m]AT").as("like_col"), df("customer"))
    val sample2 = df2.take(100)
    assert(df2.columns === Seq("like_col", "customer"))
    assert(df2.schema("like_col").storageType === BooleanType)
    assert(sample2.get[Boolean]("like_col").get.forall(!_))
  }

  test("rlike") {
    val df = getCylinderBands.limit(100)

    // use RLIKE expression: false all the time
    val df1 = df.select(df("customer").rlike("^MOD[M|m]AT").as("like_col"), df("customer"))
    val sample1 = df1.take(100)
    assert(df1.columns === Seq("like_col", "customer"))
    assert(df1.schema("like_col").storageType === BooleanType)
    assert(sample1.rows.forall {
      case Seq(s: Boolean, c: String) => s === (c === "MODMAT")
    })
  }

  test("getItem") {
    // TODO: implement this
  }

  test("getField") {
    // TODO: implement this
  }

  test("substring") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("customer").substr(2, 3).as("substr_col"), df("customer"))
    val sample1 = df1.take(100)
    assert(df1.columns === Seq("substr_col", "customer"))
    assert(df1.schema("substr_col").storageType === StringType)
    assert(sample1.rows.forall {
      case Seq(s: String, s1: String) => s === s1.substring(1, math.min(4, s1.length))
    })

    val df2 = df.select(df("customer").substr(functions.lit(1), (df("unit_number") % 4) + 1).as("substr_col"),
      df("customer"), df("unit_number"))
    val sample2 = df2.take(100)
    assert(df2.columns === Seq("substr_col", "customer", "unit_number"))
    assert(df2.schema("substr_col").storageType === StringType)
    assert(sample2.rows.forall {
      case Seq(s: String, s1: String, v: Int) => s === s1.substring(0, math.min(s1.length, 1 + (v % 4)))
    })

    // invalid starting index will give empty strings
    val df3 = df.select(df("customer").substr(-100, 2).as("substr_col"), df("customer"))
    assert(df3.take(100).get[String]("substr_col").get.forall(_ === ""))

    // len = big number will give the whole string
    val df4 = df.select(df("customer").substr(-100, 102).as("substr_col"), df("customer"))
    assert(df4.take(100).rows.forall {
      case Seq(s: String, s1: String) => s === s1
    })

    // automatically wrap at the last index
    val df5 = df.select(df("customer").substr(2, 100).as("substr_col"), df("customer"))
    assert(df5.take(100).rows.forall {
      case Seq(s: String, s1: String) => s === s1.substring(1)
    })
  }

  test("contains") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("customer").contains("MOD").as("contains_col"), df("customer"))
    val sample1 = df1.take(100)
    assert(df1.columns === Seq("contains_col", "customer"))
    assert(df1.schema("contains_col").storageType === BooleanType)
    assert(sample1.rows.forall {
      case Seq(s: Boolean, s1: String) => s === s1.contains("MOD")
    })

    val df2 = df.select(df("customer").contains(df("customer").substr(1, 2)).as("contains_col"), df("customer"))
    val sample2 = df2.take(100)
    assert(df2.columns === Seq("contains_col", "customer"))
    assert(df2.schema("contains_col").storageType === BooleanType)
    assert(sample2.get[Boolean]("contains_col").get.forall(p => p))
  }

  test("startsWith") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("customer").startsWith("MOD").as("start_col"), df("customer"))
    val sample1 = df1.take(100)
    assert(df1.columns === Seq("start_col", "customer"))
    assert(df1.schema("start_col").storageType === BooleanType)
    assert(sample1.rows.forall {
      case Seq(s: Boolean, s1: String) => s === s1.startsWith("MOD")
    })

    val df2 = df.select(df("customer").startsWith(df("customer").substr(1, 2)).as("start_col"), df("customer"))
    val sample2 = df2.take(100)
    assert(df2.columns === Seq("start_col", "customer"))
    assert(df2.schema("start_col").storageType === BooleanType)
    assert(sample2.get[Boolean]("start_col").get.forall(p => p))
  }

  test("endsWith") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("customer").endsWith("MAT").as("end_col"), df("customer"))
    val sample1 = df1.take(100)
    assert(df1.columns === Seq("end_col", "customer"))
    assert(df1.schema("end_col").storageType === BooleanType)
    assert(sample1.rows.forall {
      case Seq(s: Boolean, s1: String) => s === s1.endsWith("MAT")
    })

    val df2 = df.select(df("customer").endsWith(df("customer").substr(2, 100)).as("end_col"), df("customer"))
    val sample2 = df2.take(100)
    assert(df2.columns === Seq("end_col", "customer"))
    assert(df2.schema("end_col").storageType === BooleanType)
    assert(sample2.get[Boolean]("end_col").get.forall(p => p))
  }

  test("as - MultiAlias") {
    // TODO: implement this
  }

  test("cast") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("job_number").cast(StringType).as("job_number_str"), df("job_number"))
    val sample1 = df1.take(100)
    assert(df1.schema("job_number_str").storageType === StringType)
    assert(sample1.rows.forall {
      case Seq(s: String, n: Int) => s === s"$n"
    })

    val df2 = df.select(df("customer").cast(FloatType).as("customer_float"), df("customer"))
    val sample2 = df2.take(100)
    assert(df2.schema("customer_float").storageType === FloatType)
    assert(sample2("customer_float").forall(_ === null))
  }

  test("bitwiseOr") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("press").bitwiseOR(10).as("press_or2"), df("press"))
    val sample1 = df1.take(100)
    assert(df1.schema("press_or2").storageType === IntegerType)
    assert(sample1.rows.forall {
      case Seq(v: Int, v1: Int) => v === (v1 | 10)
    })

    val df2 = df.select(df("press").bitwiseOR(df("unit_number")).as("or_col"), df("press"), df("unit_number"))
    val sample2 = df2.take(100)
    assert(df2.schema("or_col").storageType === IntegerType)
    assert(sample2.rows.forall {
      case Seq(v: Int, v1: Int, v2: Int) => v === (v1 | v2)
    })

    // or with a string column gives exception
    intercept[CebesBackendException] {
      df.select(df("customer").bitwiseOR(df("press")).as("or_col"), df("press"), df("customer"))
    }
  }

  test("bitwiseAnd") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("press").bitwiseAND(10).as("press_and"), df("press"))
    val sample1 = df1.take(100)
    assert(df1.schema("press_and").storageType === IntegerType)
    assert(sample1.rows.forall {
      case Seq(v: Int, v1: Int) => v === (v1 & 10)
    })

    val df2 = df.select(df("press").bitwiseAND(df("unit_number")).as("and_col"), df("press"), df("unit_number"))
    val sample2 = df2.take(100)
    assert(df2.schema("and_col").storageType === IntegerType)
    assert(sample2.rows.forall {
      case Seq(v: Int, v1: Int, v2: Int) => v === (v1 & v2)
    })

    // and with a string column gives exception
    intercept[CebesBackendException] {
      df.select(df("customer").bitwiseAND(df("press")).as("and_col"), df("press"), df("customer"))
    }
  }

  test("bitwiseXor") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("press").bitwiseXOR(10).as("press_xor"), df("press"))
    val sample1 = df1.take(100)
    assert(df1.schema("press_xor").storageType === IntegerType)
    assert(sample1.rows.forall {
      case Seq(v: Int, v1: Int) => v === (v1 ^ 10)
    })

    val df2 = df.select(df("press").bitwiseXOR(df("unit_number")).as("xor_col"), df("press"), df("unit_number"))
    val sample2 = df2.take(100)
    assert(df2.schema("xor_col").storageType === IntegerType)
    assert(sample2.rows.forall {
      case Seq(v: Int, v1: Int, v2: Int) => v === (v1 ^ v2)
    })

    // or with a string column gives exception
    intercept[CebesBackendException] {
      df.select(df("customer").bitwiseXOR(df("press")).as("xor_col"), df("press"), df("customer"))
    }
  }
}
