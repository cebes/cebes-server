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
 * Created by phvu on 22/11/2016.
 */

package io.cebes.spark.df

import io.cebes.common.CebesBackendException
import io.cebes.df.functions
import io.cebes.df.types.storage._
import io.cebes.spark.helpers.TestDataHelper
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class SparkGroupedDataframeSuite extends FunSuite with BeforeAndAfterAll with TestDataHelper {

  override def beforeAll(): Unit = {
    super.beforeAll()
    createOrReplaceCylinderBands()
  }

  test("agg") {
    val gdf = getCylinderBands.limit(100).groupBy("customer")

    val df1 = gdf.agg(("cylinder_number", "count"),
      ("job_number", "max"))
    assert(df1.numCols === 3)
    assert(df1.numRows > 0)

    val df2 = gdf.agg(Map("cylinder_number" -> "count", "job_number" -> "max"))
    assert(df2.numCols === 3)
    assert(df2.numRows > 0)

    val df3 = gdf.agg(functions.count("cylinder_number"), functions.max("job_number"))
    assert(df3.numCols === 3)
    assert(df3.numRows > 0)
  }

  test("count") {
    val gdf = getCylinderBands.limit(100).groupBy("customer")

    val df1 = gdf.count()
    assert(df1.numCols === 2)
    assert(df1.numRows > 0)
    assert(df1.schema.last.storageType === LongType)
  }

  test("max") {
    val gdf = getCylinderBands.limit(100).groupBy("customer")

    val df1 = gdf.max("job_number", "proof_cut")
    assert(df1.numCols === 3)
    assert(df1.numRows > 0)
    assert(df1.schema(1).storageType === IntegerType)
    assert(df1.schema.last.storageType === FloatType)


    val ex1 = intercept[CebesBackendException] {
      gdf.max("cylinder_number")
    }
    assert(ex1.message.contains("\"cylinder_number\" is not a numeric column"))
  }

  test("min") {
    val gdf = getCylinderBands.limit(100).groupBy("customer")

    val df1 = gdf.min("job_number", "proof_cut")
    assert(df1.numCols === 3)
    assert(df1.numRows > 0)
    assert(df1.schema(1).storageType === IntegerType)
    assert(df1.schema.last.storageType === FloatType)


    val ex1 = intercept[CebesBackendException] {
      gdf.min("cylinder_number")
    }
    assert(ex1.message.contains("\"cylinder_number\" is not a numeric column"))
  }

  test("avg") {
    val gdf = getCylinderBands.limit(100).groupBy("customer")

    val df1 = gdf.avg("job_number", "proof_cut")
    assert(df1.numCols === 3)
    assert(df1.numRows > 0)
    assert(df1.schema(1).storageType === DoubleType)
    assert(df1.schema.last.storageType === DoubleType)


    val ex1 = intercept[CebesBackendException] {
      gdf.mean("cylinder_number")
    }
    assert(ex1.message.contains("\"cylinder_number\" is not a numeric column"))
  }

  test("sum") {
    val gdf = getCylinderBands.limit(100).groupBy("customer")

    val df1 = gdf.sum("job_number", "proof_cut")
    assert(df1.numCols === 3)
    assert(df1.numRows > 0)
    assert(df1.schema(1).storageType === LongType)
    assert(df1.schema.last.storageType === DoubleType)


    val ex1 = intercept[CebesBackendException] {
      gdf.sum("cylinder_number")
    }
    assert(ex1.message.contains("\"cylinder_number\" is not a numeric column"))
  }

  test("pivot") {
    val gdf = getCylinderBands.limit(100).groupBy("customer")

    val df1 = gdf.pivot("grain_screened").min("job_number")
    assert(df1.numCols === 4)
    assert(df1.numRows > 0)
    assert(df1.schema.head.storageType === StringType)
    assert(df1.schema.tail.forall(_.storageType === IntegerType))

    val df2 = gdf.pivot("grain_screened", Seq("YES", "NO")).min("job_number")
    assert(df2.numCols === 3)
    assert(df2.numRows > 0)
    assert(df2.schema.head.storageType === StringType)
    assert(df2.schema.tail.forall(_.storageType === IntegerType))

    val df3 = gdf.pivot("grain_screened", Seq("YES", "NO")).agg(
      Map("job_number" -> "min", "caliper" -> "sum"))
    assert(df3.numCols === 5)
    assert(df3.numRows > 0)
  }
}
