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

import java.sql.Timestamp

import io.cebes.common.CebesBackendException
import io.cebes.df.types.storage._
import io.cebes.df.types.{StorageTypes, VariableTypes}
import io.cebes.df.{Column, functions}
import io.cebes.spark.helpers.{TestDataHelper, TestPropertyHelper}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.mutable

class SparkDataframeSuite extends FunSuite with BeforeAndAfterAll
  with TestPropertyHelper with TestDataHelper {

  override def beforeAll(): Unit = {
    super.beforeAll()
    createOrReplaceCylinderBands()
  }


  ////////////////////////////////////////////////////////////////////////////////////
  // Variable types
  ////////////////////////////////////////////////////////////////////////////////////

  test("Dataframe variable types") {
    val df = getCylinderBands
    assert(df.schema("customer").storageType === StorageTypes.StringType)
    assert(df.schema("customer").variableType === VariableTypes.TEXT)

    val df2 = df.inferVariableTypes()
    assert(df2.id !== df.id)
    assert(Seq(VariableTypes.TEXT, VariableTypes.NOMINAL).contains(df2.schema("customer").variableType))
    assert(df2.schema("job_number").variableType === VariableTypes.ORDINAL)

    val df3 = df.withVariableTypes(Map("customer" -> VariableTypes.ORDINAL,
      "Job_Number" -> VariableTypes.DISCRETE))
    assert(df3.id !== df.id)
    assert(df3.schema("customer").variableType === VariableTypes.ORDINAL)
    assert(df3.schema("job_number").variableType === VariableTypes.DISCRETE)
  }

  test("Apply schema") {
    val df = getCylinderBands
    assert(df.schema("job_number").storageType === IntegerType)

    val newSchema = df.schema.withFieldRenamed("timestamp", "new_timestamp").
      withField("JoB_number", LongType, VariableTypes.NOMINAL)
    val df2 = df.applySchema(newSchema)
    assert(df2.id !== df.id)
    assert(!df2.columns.contains("timestamp"))
    assert(df2.columns.contains("new_timestamp"))
    assert(df2.schema("Job_number").storageType === LongType)
    assert(df2.schema("Job_number").variableType === VariableTypes.NOMINAL)

    intercept[IllegalArgumentException] {
      df.applySchema(df.schema.withField("newColumn", IntegerType))
    }
  }

  ////////////////////////////////////////////////////////////////////////////////////
  // Sampling
  ////////////////////////////////////////////////////////////////////////////////////

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
      s"FROM $cylinderBandsTableName LIMIT 10")
    assert(df.numCols === 13)

    val sample = df.take(10)
    assert(sample.numCols === 13)
    sample.data.foreach { c =>
      assert(c.length === 10)
      assert(!c.forall(_ === null))
    }
    assert(sample.tabulate().length > 0)
  }

  test("sample") {
    val df = getCylinderBands
    assert(df.numCols === 40)
    assert(df.numRows === 540)

    val df2 = df.sample(withReplacement = false, 0.1, 42)
    assert(df2.numCols === df.numCols)
    assert(df2.numRows > 0)

    val df3 = df.sample(withReplacement = true, 2.0, 42)
    assert(df3.numCols === df.numCols)
    assert(df3.numRows > df.numRows)
  }

  ////////////////////////////////////////////////////////////////////////////////////
  // Data exploration
  ////////////////////////////////////////////////////////////////////////////////////

  test("sort") {
    val df = getCylinderBands

    // ascending
    val df2 = df.sort(df.col("timestamp"))
    assert(df2.id !== df.id)
    val sample2 = df2.take(100)
    val col2 = sample2.get[Long]("Timestamp").get
    assert((1 until col2.length).forall(i => col2(i) >= col2(i - 1)))

    // descending
    val df3 = df.sort(df.col("timestamp").desc)
    val sample3 = df3.take(100)
    val col3 = sample3.get[Long]("Timestamp").get
    assert((1 until col3.length).forall(i => col3(i) <= col3(i - 1)))

    // mixed
    val df4 = df.sort(df.col("timestamp").desc, df.col("cylinder_number").asc)
    val sample4 = df4.take(10)
    val col4Ts = sample4.get[Long]("Timestamp").get
    val col4Cn = sample4.get[String]("cylinder_number").get
    assert(col4Ts.head === col4Ts(1))
    assert(col4Cn.head < col4Cn(1))
    assert(col4Ts.head > col4Ts.last)

    val ex = intercept[CebesBackendException] {
      df.sort(df.col("timestampZXXXXX"))
    }
    assert(ex.getMessage.startsWith("Spark query analysis exception: Cannot resolve column name"))
  }

  test("drop columns") {
    val df = getCylinderBands
    val df2 = df.drop(Seq("random_columns"))
    assert(df2.id === df.id)

    val df3 = df.drop(Seq("customer", "job_number"))
    assert(df3.numCols === df.numCols - 2)
    assert(df3.numRows === df.numRows)

    // column names are case-insensitive
    val df4 = df.drop(Seq("CusTomer", "Job_NumBer"))
    assert(df4.numCols === df.numCols - 2)
    assert(df4.numRows === df.numRows)
  }

  test("dropDuplicates") {
    val df = getCylinderBands.limit(30)
    val df2 = df.dropDuplicates(df.columns)
    assert(df.id !== df2.id)
    assert(df.numRows === df2.numRows)

    val df3 = df2.union(df2)
    assert(df3.numRows === 2 * df2.numRows)
    assert(df3.numCols === df2.numCols)

    val df4 = df3.dropDuplicates(df3.columns)
    assert(df4.numCols === df3.numCols)
    assert(df4.numRows === df2.numRows)

    val df5 = df3.distinct()
    assert(df5.numCols === df3.numCols)
    assert(df5.numRows === df2.numRows)
  }

  ////////////////////////////////////////////////////////////////////////////////////
  // SQL-like APIs
  ////////////////////////////////////////////////////////////////////////////////////

  test("withColumn") {
    val dfOriginal = getCylinderBands

    // add new column
    val df1 = dfOriginal.withColumn("timestamp2", dfOriginal("Timestamp"))
    val sample1 = df1.take(10)
    assert(df1.id !== dfOriginal.id)
    assert(df1.numCols === dfOriginal.numCols + 1)
    assert(df1.columns.last === "timestamp2")
    assert(sample1.get[Long]("timestamp") === sample1.get[Long]("timestamp2"))

    // replace a column
    val df3 = dfOriginal.withColumn("timestamp", dfOriginal("job_number"))
    val sample3 = df3.take(10)
    assert(df3.numCols === dfOriginal.numCols)
    assert(df3.schema("timestamp").storageType === dfOriginal.schema("job_number").storageType)
    assert(df3.schema("timestamp").variableType === dfOriginal.schema("job_number").variableType)
    assert(sample3.get[Long]("timestamp") === sample3.get[Long]("job_number"))

    // invalid use of wildcard
    val exception2 = intercept[CebesBackendException] {
      dfOriginal.withColumn("all_of_them", dfOriginal("*"))
    }
    assert(exception2.message.contains("Invalid usage of '*'"))
  }

  test("withColumnRenamed") {
    val df = getCylinderBands

    // correct case
    val df1 = df.withColumnRenamed("Timestamp", "timestamp_new")
    assert(df1.id !== df.id)
    assert(df1.numCols === df.numCols)
    assert(df1.schema.get("timestamp").isEmpty)
    assert(df1.schema.get("Timestamp_new").nonEmpty)
    assert(df1.columns.contains("timestamp_new"))
    assert(!df1.columns.contains("timestamp"))

    // rename to an existing name is fine, but error when it gets selected
    val df2 = df.withColumnRenamed("Timestamp", "job_number")
    val exception2 = intercept[CebesBackendException](df2("job_number"))
    assert(exception2.message.contains("Reference 'job_number' is ambiguous"))

    // existing column name is not in the df
    val df3 = df.withColumnRenamed("timestamp_not_existed", "timestamp_new")
    assert(df3.columns === df.columns)
  }

  test("select") {
    val df = getCylinderBands

    // select single column, case-insensitive name
    val df1 = df.select(df("TimeStamp"))
    assert(df1.numCols === 1)
    assert(df1.columns.head === "TimeStamp")
    assert(df1.schema("timestamp").storageType === df.schema("timestamp").storageType)

    // select 3 columns
    val df2 = df.select(df("roughness"), df("blade_pressure"), df("varnish_pct"))
    assert(df2.numCols === 3)
    assert(df2.columns === Seq("roughness", "blade_pressure", "varnish_pct"))
    assert(df2.schema("roughness").storageType === df.schema("roughness").storageType)
    assert(df2.schema("blade_pressure").storageType === df.schema("blade_pressure").storageType)
    assert(df2.schema("varnish_pct").storageType === df.schema("varnish_pct").storageType)

    // select everything
    val df3 = df.select(df("*"))
    assert(df3.numCols === df.numCols)
    assert(df3.columns === df.columns)
    assert(df3.columns.forall(s => df3.schema(s).storageType === df.schema(s).storageType))

    // select a non-existing column
    intercept[CebesBackendException](df.select(df("non_existing_column")))
  }

  test("where") {
    val df = getCylinderBands.where(functions.col("job_number") % 2 === 0 && functions.col("grain_screened") === "YES")
    val sample = df.take(100)
    val jobColIdx = df.columns.indexOf("job_number")
    val grainColIdx = df.columns.indexOf("grain_screened")
    assert(df.numRows > 0)
    assert(sample.rows.forall { s =>
      s(jobColIdx).asInstanceOf[Int] % 2 === 0 && s(grainColIdx) === "YES"
    })
  }

  test("orderBy") {
    // very light test because orderBy is an alias of sort
    val df = getCylinderBands
    val df2 = df.orderBy(df("timestamp"))
    assert(df2.id !== df.id)
    val sample2 = df2.take(100)
    val col2 = sample2.get[Long]("Timestamp").get
    assert((1 until col2.length).forall(i => col2(i) >= col2(i - 1)))
  }

  test("alias") {
    val df = getCylinderBands

    val df1 = df.alias("new_name")
    assert(df1.id !== df.id)
    assert(df1.columns === df.columns)
    assert(df1.numRows === df.numRows)

    val df2 = df.alias("string with space")
    assert(df2.id !== df.id)
    assert(df2.columns === df.columns)
    assert(df2.numRows === df.numRows)
  }

  test("join") {
    val df = getCylinderBands

    val df1 = df.select("*").where(df("customer").isin("GUIDEPOSTS", "ECKERD")).alias("small")
    val df2 = df.select("*").where(df("customer").isin("GUIDEPOSTS", "ECKERD", "TARGET")).alias("big")

    // inner join
    val dfj1 = df1.join(df2, functions.col("small.customer") === functions.col("big.customer"), "inner")
      .select(df1("customer").as("small_customer"), df1("cylinder_number"),
        df2("customer").as("big_customer"), df2("ink_color"))
    assert(dfj1.numRows === 117)
    assert(dfj1.columns === Seq("small_customer", "cylinder_number", "big_customer", "ink_color"))

    // outer join
    val dfj2 = df1.join(df2, functions.col("small.customer") === functions.col("big.customer"), "outer")
      .select(df1("customer").as("small_customer"), df1("cylinder_number"),
        df2("customer").as("big_customer"), df2("ink_color"))
    assert(dfj2.numRows === 157)
    assert(dfj2.columns === Seq("small_customer", "cylinder_number", "big_customer", "ink_color"))

    intercept[IllegalArgumentException] {
      df1.join(df2, functions.col("small.customer") === functions.col("big.customer"), "outer_unrecognized")
    }

    // join with broadcast
    val dfj3 = df1.broadcast.join(df2, functions.col("small.customer") === functions.col("big.customer"), "inner")
      .select(df1("customer").as("small_customer"), df1("cylinder_number"),
        df2("customer").as("big_customer"), df2("ink_color"))
    assert(dfj3.numRows === 117)
    assert(dfj3.columns === Seq("small_customer", "cylinder_number", "big_customer", "ink_color"))

    val dfj4 = df1.join(functions.broadcast(df2),
      functions.col("small.customer") === functions.col("big.customer"), "inner")
      .select(df1("customer").as("small_customer"), df1("cylinder_number"),
        df2("customer").as("big_customer"), df2("ink_color"))
    assert(dfj4.numRows === 117)
    assert(dfj4.columns === Seq("small_customer", "cylinder_number", "big_customer", "ink_color"))
  }

  test("limit") {
    val df = getCylinderBands
    val df2 = df.limit(20)
    assert(df2.columns === df.columns)
    assert(df2.numRows === 20)

    intercept[IllegalArgumentException] {
      df.limit(-1)
    }
  }

  test("union") {
    val df = getCylinderBands.limit(20)

    val df2 = df.union(df)
    assert(df2.numRows === 2 * df.numRows)
    assert(df2.numCols === df.numCols)

    val df3 = df.drop(Seq("customer", "job_number"))
    assert(df3.numCols === df.numCols - 2)

    intercept[IllegalArgumentException] {
      df.union(df3)
    }
  }

  test("intersect") {

    // expected case
    val df1 = sparkDataframeService.sql(s"SELECT * FROM $cylinderBandsTableName WHERE customer LIKE 'BELK'")
    val df2 = sparkDataframeService.sql(s"SELECT * FROM $cylinderBandsTableName WHERE " +
      s"customer LIKE 'BELK' OR customer LIKE 'AMES'")
    assert(df1.numRows === 4)
    val df3 = df1.intersect(df2)
    assert(df3.numRows === 4)

    // self intersect
    val df4 = sparkDataframeService.sql(s"SELECT * FROM $cylinderBandsTableName LIMIT 30")
    val df5 = df4.intersect(df4)
    assert(df5.numRows === df4.numRows)

    val df6 = df4.drop(Seq("customer", "job_number"))
    intercept[IllegalArgumentException] {
      df4.intersect(df6)
    }
  }

  test("except") {

    // expected case
    val df1 = sparkDataframeService.sql(s"SELECT * FROM $cylinderBandsTableName WHERE customer LIKE 'BELK'")
    val df2 = sparkDataframeService.sql(s"SELECT * FROM $cylinderBandsTableName WHERE " +
      s"customer LIKE 'BELK' OR customer LIKE 'AMES'")
    assert(df1.numRows === 4)
    val df3 = df2.except(df1)
    assert(df3.numRows === df2.numRows - df1.numRows)

    // self except
    val df4 = sparkDataframeService.sql(s"SELECT * FROM $cylinderBandsTableName LIMIT 30")
    val df5 = df4.except(df4)
    assert(df5.numRows === 0)

    val df6 = df4.drop(Seq("customer", "job_number"))
    intercept[IllegalArgumentException] {
      df4.except(df6)
    }
  }

  test("groupBy") {
    val df = getCylinderBands

    val df1 = df.groupBy("customer", "grain_screened").avg()
    assert(df1.columns.head === "customer")
    assert(df1.columns(1) === "grain_screened")
    assert(df1.numCols > 2)
    assert(df1.numRows > 0)

    val df2 = df.groupBy(df("customer"), df("job_number") % 2).max()
    assert(df2.numCols > 2)
    assert(df2.numRows > 0)

    intercept[CebesBackendException] {
      df.groupBy("non_existed_column").max()
    }
  }

  test("rollUp") {
    val df = getCylinderBands

    val df1 = df.rollup(df("customer"), df("grain_screened")).avg()
    assert(df1.columns.head === "customer")
    assert(df1.columns(1) === "grain_screened")
    assert(df1.numCols > 2)
    assert(df1.numRows > 0)

    intercept[CebesBackendException] {
      df.rollup(df("non_existed_column")).max()
    }
  }

  test("cube") {
    val df = getCylinderBands

    val df1 = df.cube(df("customer"), df("grain_screened")).avg()
    assert(df1.columns.head === "customer")
    assert(df1.columns(1) === "grain_screened")
    assert(df1.numCols > 2)
    assert(df1.numRows > 0)

    intercept[CebesBackendException] {
      df.cube(df("non_existed_column")).max()
    }
  }

  test("agg") {
    val df = getCylinderBands

    val df1 = df.agg(("timestamp", "max"), ("press", "avg"))
    assert(df1.numCols === 2)
    assert(df1.numRows === 1)

    intercept[CebesBackendException] {
      df.agg(Map("timestamp" -> "max", "press" -> "avgggg"))
    }

    // not an aggregation function
    intercept[CebesBackendException] {
      df.agg(df("timestamp"))
    }
  }

  test("agg - approxCountDistinct") {
    val df = getCylinderBands

    val df1 = df.agg(functions.approxCountDistinct("customer"),
      functions.approxCountDistinct("customer", rsd = 0.05))
    assert(df1.numCols === 2)
    assert(df1.numRows === 1)

    val df2 = df.agg(functions.approxCountDistinct(df("customer")),
      functions.approxCountDistinct(df("customer"), rsd = 0.05))
    assert(df2.numCols === 2)
    assert(df2.numRows === 1)

    // rsd is too high
    intercept[IllegalArgumentException] {
      df.agg(functions.approxCountDistinct("customer", rsd = 0.5))
    }
  }

  test("agg - avg") {
    val df = getCylinderBands

    val df1 = df.agg(functions.avg("caliper"), functions.avg(df("caliper") / 2))
    assert(df1.numCols === 2)
    assert(df1.numRows === 1)
    assert(df1.columns.forall(c => df1.schema(c).storageType === DoubleType))

    // avg on a string column still gives result, but it is null
    assert(df.agg(functions.avg("customer")).numRows === 1)
  }

  test("agg - collectList") {
    val df = getCylinderBands.limit(30)

    val df1 = df.agg(functions.collectList("customer"),
      functions.collectList(df("job_number") / 2))
    assert(df1.numCols === 2)
    assert(df1.numRows === 1)
    val sample = df1.take(10)
    assert(sample.data.length == 2)
    assert(sample.data.head.head.isInstanceOf[mutable.WrappedArray[_]])
    assert(sample.data.last.head.isInstanceOf[mutable.WrappedArray[_]])
  }

  test("agg - collectSet") {
    val df = getCylinderBands.limit(30)

    val df1 = df.agg(functions.collectSet("customer"),
      functions.collectSet(df("job_number") / 2))
    assert(df1.numCols === 2)
    assert(df1.numRows === 1)
    val sample = df1.take(10)
    assert(sample.data.length == 2)
    assert(sample.data.head.head.isInstanceOf[mutable.WrappedArray[_]])
    assert(sample.data.last.head.isInstanceOf[mutable.WrappedArray[_]])
  }

  test("agg - corr") {
    val df = getCylinderBands.limit(30)

    val df1 = df.agg(functions.corr("job_number", "press"),
      functions.corr("job_number", "job_number"),
      functions.corr(df("job_number") / 2, df("customer")))
    assert(df1.numCols === 3)
    assert(df1.numRows === 1)
  }

  test("agg - count") {
    val df = getCylinderBands.limit(30)

    val df1 = df.agg(functions.count("*"),
      functions.count(df("proof_on_ctd_ink")))
    assert(df1.numCols === 2)
    assert(df1.numRows === 1)
  }

  test("agg - countDistinct") {
    val df = getCylinderBands.limit(30)

    val df1 = df.agg(functions.countDistinct("customer", "proof_on_ctd_ink"),
      functions.countDistinct(df("customer")))
    assert(df1.numCols === 2)
    assert(df1.numRows === 1)

    // * does not work with count distinct
    intercept[CebesBackendException] {
      df.agg(functions.countDistinct("*"))
    }
    intercept[CebesBackendException] {
      df.agg(functions.countDistinct(df("*")))
    }
  }

  test("agg - covarPop") {
    val df = getCylinderBands.limit(30)

    val df1 = df.agg(functions.covarPop("press", "job_number"),
      functions.covarPop("press", "press"),
      functions.covarPop("customer", "press"),
      functions.covarPop(df("job_number"), df("press") / 2))
    assert(df1.numCols === 4)
    assert(df1.numRows === 1)
  }

  test("agg - covarSamp") {
    val df = getCylinderBands.limit(30)

    val df1 = df.agg(functions.covarSamp("press", "job_number"),
      functions.covarSamp("press", "press"),
      functions.covarSamp("customer", "press"),
      functions.covarSamp(df("job_number"), df("press") / 2))
    assert(df1.numCols === 4)
    assert(df1.numRows === 1)
  }

  test("agg - first") {
    val df = getCylinderBands.limit(30)

    val df1 = df.agg(functions.first("press", ignoreNulls = true),
      functions.first("customer"),
      functions.first(df("customer"), ignoreNulls = true),
      functions.first(df("job_number")))
    assert(df1.numCols === 4)
    assert(df1.numRows === 1)
  }

  test("agg - last") {
    val df = getCylinderBands.limit(30)

    val df1 = df.agg(functions.last("press", ignoreNulls = true),
      functions.last("customer"),
      functions.last(df("customer"), ignoreNulls = true),
      functions.last(df("job_number")))
    assert(df1.numCols === 4)
    assert(df1.numRows === 1)
  }

  test("agg - grouping") {
    val df = getCylinderBands.limit(30)

    val df1 = df.cube(df("press"), df("customer")).agg(
      functions.grouping("press"),
      functions.grouping(df("customer")))
    assert(df1.numCols === 4)
    assert(df1.numRows > 0)

    // grouping only works with cube/rollup/groupingSet
    intercept[CebesBackendException] {
      df.agg(functions.grouping("press"),
        functions.grouping(df("customer")))
    }
  }

  test("agg - groupingId") {
    val df = getCylinderBands.limit(30)

    val df1 = df.cube(df("press"), df("customer")).agg(
      functions.groupingId("press", "customer"),
      functions.groupingId(df("press"), df("customer")))
    assert(df1.numCols === 4)
    assert(df1.numRows > 0)

    val ex1 = intercept[CebesBackendException] {
      df.agg(functions.groupingId("press"),
        functions.groupingId(df("customer")))
    }
    assert(ex1.message.contains("grouping_id() can only be used with GroupingSets/Cube/Rollup"))

    // Columns of grouping_id does not match grouping columns
    val ex2 = intercept[CebesBackendException] {
      df.cube(df("press"), df("customer")).agg(
        functions.groupingId("press"),
        functions.groupingId(df("customer")))
    }
    assert(ex2.message.contains("Columns of grouping_id"))
    assert(ex2.message.contains("does not match grouping columns"))
  }

  test("agg - kurtosis") {
    val df = getCylinderBands.limit(30)

    val df1 = df.agg(functions.kurtosis("caliper"),
      functions.kurtosis(df("caliper") / 2))
    assert(df1.numCols === 2)
    assert(df1.numRows === 1)
    assert(df1.columns.forall(c => df1.schema(c).storageType === DoubleType))

    // avg on a string column still gives result, but it is null
    assert(df.agg(functions.kurtosis("customer")).numRows === 1)
  }

  test("agg - max") {
    val df = getCylinderBands.limit(30)

    val df1 = df.agg(functions.max("caliper"),
      functions.max(df("caliper") / 2))
    assert(df1.numCols === 2)
    assert(df1.numRows === 1)
    assert(df1.schema.head.storageType === FloatType)
    assert(df1.schema.last.storageType === DoubleType)

    // avg on a string column still gives result, but it is null
    assert(df.agg(functions.max("customer")).numRows === 1)
  }

  test("agg - mean") {
    val df = getCylinderBands.limit(30)

    val df1 = df.agg(functions.mean("caliper"),
      functions.mean(df("caliper") / 2))
    assert(df1.numCols === 2)
    assert(df1.numRows === 1)
    assert(df1.columns.forall(c => df1.schema(c).storageType === DoubleType))

    // avg on a string column still gives result, but it is null
    assert(df.agg(functions.mean("customer")).numRows === 1)
  }

  test("agg - min") {
    val df = getCylinderBands.limit(30)

    val df1 = df.agg(functions.min("caliper"),
      functions.min(df("caliper") / 2))
    assert(df1.numCols === 2)
    assert(df1.numRows === 1)
    assert(df1.schema.head.storageType === FloatType)
    assert(df1.schema.last.storageType === DoubleType)

    // avg on a string column still gives result, but it is null
    assert(df.agg(functions.min("customer")).numRows === 1)
  }

  test("agg - skewness") {
    val df = getCylinderBands.limit(30)

    val df1 = df.agg(functions.skewness("caliper"),
      functions.skewness(df("caliper") / 2))
    assert(df1.numCols === 2)
    assert(df1.numRows === 1)
    assert(df1.columns.forall(c => df1.schema(c).storageType === DoubleType))

    // avg on a string column still gives result, but it is null
    assert(df.agg(functions.skewness("customer")).numRows === 1)
  }

  test("agg - stddev") {
    val df = getCylinderBands.limit(30)

    val df1 = df.agg(functions.stddev("caliper"),
      functions.stddev(df("caliper") / 2))
    assert(df1.numCols === 2)
    assert(df1.numRows === 1)
    assert(df1.columns.forall(c => df1.schema(c).storageType === DoubleType))

    // avg on a string column still gives result, but it is null
    assert(df.agg(functions.stddev("customer")).numRows === 1)
  }

  test("agg - stddevPop") {
    val df = getCylinderBands.limit(30)

    val df1 = df.agg(functions.stddevPop("caliper"),
      functions.stddevPop(df("caliper") / 2))
    assert(df1.numCols === 2)
    assert(df1.numRows === 1)
    assert(df1.columns.forall(c => df1.schema(c).storageType === DoubleType))

    // avg on a string column still gives result, but it is null
    assert(df.agg(functions.stddevPop("customer")).numRows === 1)
  }

  test("agg - stddevSamp") {
    val df = getCylinderBands.limit(30)

    val df1 = df.agg(functions.stddevSamp("caliper"),
      functions.stddevSamp(df("caliper") / 2))
    assert(df1.numCols === 2)
    assert(df1.numRows === 1)
    assert(df1.columns.forall(c => df1.schema(c).storageType === DoubleType))

    // avg on a string column still gives result, but it is null
    assert(df.agg(functions.stddevSamp("customer")).numRows === 1)
  }

  test("agg - sum") {
    val df = getCylinderBands.limit(30)

    val df1 = df.agg(functions.sum("caliper"),
      functions.sum(df("caliper") / 2))
    assert(df1.numCols === 2)
    assert(df1.numRows === 1)
    assert(df1.columns.forall(c => df1.schema(c).storageType === DoubleType))

    // avg on a string column still gives result, but it is null
    assert(df.agg(functions.sum("customer")).numRows === 1)
  }

  test("agg - sumDistinct") {
    val df = getCylinderBands.limit(30)

    val df1 = df.agg(functions.sumDistinct("caliper"),
      functions.sumDistinct(df("caliper") / 2))
    assert(df1.numCols === 2)
    assert(df1.numRows === 1)
    assert(df1.columns.forall(c => df1.schema(c).storageType === DoubleType))

    // avg on a string column still gives result, but it is null
    assert(df.agg(functions.sumDistinct("customer")).numRows === 1)
  }

  test("agg - variance") {
    val df = getCylinderBands.limit(30)

    val df1 = df.agg(functions.variance("caliper"),
      functions.variance(df("caliper") / 2))
    assert(df1.numCols === 2)
    assert(df1.numRows === 1)
    assert(df1.columns.forall(c => df1.schema(c).storageType === DoubleType))

    // avg on a string column still gives result, but it is null
    assert(df.agg(functions.variance("customer")).numRows === 1)
  }

  test("agg - varSamp") {
    val df = getCylinderBands.limit(30)

    val df1 = df.agg(functions.varSamp("caliper"),
      functions.varSamp(df("caliper") / 2))
    assert(df1.numCols === 2)
    assert(df1.numRows === 1)
    assert(df1.columns.forall(c => df1.schema(c).storageType === DoubleType))

    // avg on a string column still gives result, but it is null
    assert(df.agg(functions.varSamp("customer")).numRows === 1)
  }

  test("agg - varPop") {
    val df = getCylinderBands.limit(30)

    val df1 = df.agg(functions.varPop("caliper"),
      functions.varPop(df("caliper") / 2))
    assert(df1.numCols === 2)
    assert(df1.numRows === 1)
    assert(df1.columns.forall(c => df1.schema(c).storageType === DoubleType))

    // avg on a string column still gives result, but it is null
    assert(df.agg(functions.varPop("customer")).numRows === 1)
  }

  test("non-agg - array") {
    val df = getCylinderBands.limit(30)

    // array of strings
    val df1 = df.select(functions.array("customer", "ink_color", "proof_on_ctd_ink").as("arr1"),
      df("customer"), df("ink_color"), df("proof_on_ctd_ink"))
    assert(df1.columns === Seq("arr1", "customer", "ink_color", "proof_on_ctd_ink"))
    assert(df1.schema("arr1").storageType === ArrayType(StringType))
    assert(df1.take(10).rows.forall {
      case Seq(s: mutable.WrappedArray[_], s1, s2, s3) => s === Seq(s1, s2, s3)
    })

    // array of floats
    val df2 = df.select(functions.array(df("caliper"), df("ink_temperature"), df("roughness")).as("arr1"))
    assert(df2.columns === Seq("arr1"))
    assert(df2.schema("arr1").storageType === ArrayType(FloatType))

    // array of mixed types
    val df3 = df.select(functions.array("customer", "caliper").as("arr1"))
    assert(df3.schema("arr1").storageType === ArrayType(StringType))
    assert(df3.schema("arr1").storageType !== ArrayType(FloatType))
  }

  test("non-agg - map") {
    val df = getCylinderBands.limit(30)

    val df1 = df.select(
      functions.map(df("customer"), df("press"), df("cylinder_number"), df("unit_number")).as("map1"),
      df("customer"), df("press"), df("cylinder_number"), df("unit_number"))
    assert(df1.columns === Seq("map1", "customer", "press", "cylinder_number", "unit_number"))
    assert(df1.schema("map1").storageType === MapType(StringType, IntegerType))
    assert(df1.take(10).rows.forall {
      case Seq(m: Map[_, _], k1, v1, k2, v2) => m === Map(k1 -> v1, k2 -> v2)
    })

    // number of columns is not even
    intercept[CebesBackendException](df.select(functions.map(df("customer"), df("press"), df("cylinder_number"))))
  }

  test("non-agg - coalesce") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(
      functions.coalesce(df("plating_tank"), df("proof_cut"), df("paper_mill_location")).as("coalesce_col"),
      df("plating_tank"), df("proof_cut"), df("paper_mill_location"))
    assert(df1.schema("coalesce_col").storageType === StringType)
    assert(df1.take(10).rows.forall {
      case Seq(v: String, null, null, v3: String) => v === v3
      case Seq(v: String, null, v2: Float, _) => v === v2.toString
      case Seq(v: String, v1: Int, _, _) => v === v1.toString
    })

    intercept[CebesBackendException] {
      df.select(functions.coalesce(df("non_existed_column"), df("plating_tank"), df("proof_cut")))
    }
  }

  test("non-agg - input_file_name") {
    val df = getCylinderBands.limit(30)

    val df1 = df.select(functions.input_file_name().as("input_file_name"), df("*"))
    assert(df1.numCols === df.numCols + 1)
    assert(df1.numRows === df.numRows)
    assert(df1.schema("input_file_name").storageType === StringType)
    println(df1.take(10).tabulate())
  }

  test("non-agg - isnull") {
    val df = getCylinderBands.limit(30)

    val df1 = df.select(df("paper_mill_location"),
      functions.when(functions.col("paper_mill_location") === "", null)
        .otherwise(functions.col("paper_mill_location")).as("pml_null"))

    val df2 = df1.select(functions.col("paper_mill_location"), functions.col("pml_null"),
      functions.isnull(functions.col("pml_null")).as("is_null"))
    assert(df2.schema("is_null").storageType === BooleanType)
    assert(df2.take(10).rows.forall {
      case Seq(_, null, v: Boolean) => v
      case Seq(_, _: String, v: Boolean) => !v
    })
  }

  test("non-agg - isnan") {
    val df = getCylinderBands.limit(30)

    val df1 = df.select(df("solvent_pct"), df("esa_voltage"),
      (df("solvent_pct") / df("esa_voltage")).cast(DoubleType).as("div_col"))
    val df2 = df1.select(functions.col("solvent_pct"), functions.col("esa_voltage"),
      functions.col("div_col"),
      functions.isnan(functions.col("div_col")).as("is_nan"),
      functions.isnull(functions.col("div_col")).as("is_null"))
    assert(df2.schema("is_nan").storageType === BooleanType)
    assert(df2.schema("is_null").storageType === BooleanType)

    // actually this test is quite bad: "div_col" is always NULL anyway, and "is_nan" is always false
    // but I don't know how to create NaNs in Spark, yet.
    assert(df2.take(10).rows.forall {
      case Seq(_, _, v: Double, vNaN: Boolean, vNull: Boolean) => vNaN === v.isNaN && !vNull
      case Seq(_, _, null, vNaN: Boolean, vNull: Boolean) => !vNaN && vNull
    })
  }

  test("non-agg - monotonically_increasing_id") {
    val df = getCylinderBands

    val df1 = df.select(functions.monotonically_increasing_id().as("increasing_id"), df("*"))
    assert(df1.numCols === df.numCols + 1)
    assert(df1.numRows === df.numRows)
    assert(df1.schema("increasing_id").storageType === LongType)
  }

  test("non-agg - nanvl") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(functions.nanvl(df("plating_tank"), df("varnish_pct")).cast(DoubleType),
      df("plating_tank").cast(DoubleType), df("varnish_pct").cast(DoubleType))
    assert(df1.numCols === 3)
    assert(df1.numRows === df.numRows)
    assert(df1.take(50).rows.forall {
      case Seq(v: Double, v1: Double, _) => v === v1
      case Seq(null, null, _) => true
    })
  }

  test("non-agg - negate") {
    val df = getCylinderBands

    val df1 = df.select(df("plating_tank"), functions.negate(df("plating_tank")).as("negate_col"))
    assert(df1.numRows === df.numRows)
    assert(df1.columns === Seq("plating_tank", "negate_col"))
    assert(df1.schema("negate_col").storageType === IntegerType)
    assert(df1.take(30).rows.forall {
      case Seq(null, null) => true
      case Seq(v1: Int, v2: Int) => v1 === -v2
    })

    // wrong data type, expect numeric but given string column
    intercept[CebesBackendException] {
      df.select(df("cylinder_number"), functions.negate(df("cylinder_number")).as("negate_col_1"))
    }
  }

  test("non-agg - not") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select((!(df("grain_screened") === df("ink_color"))).as("not_col"),
      df("grain_screened"), df("ink_color"))
    assert(df1.numRows === df.numRows)
    assert(df1.columns === Seq("not_col", "grain_screened", "ink_color"))
    assert(df1.schema("not_col").storageType === BooleanType)
    assert(df1.take(100).rows.forall {
      case Seq(v: Boolean, s1: String, s2: String) => v === !(s1 === s2)
      case Seq(v: Boolean, null, _: String) => v
      case Seq(v: Boolean, _: String, null) => v
      case Seq(v: Boolean, null, null) => !v
    })
  }

  test("non-agg - rand and randn") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(functions.rand().as("rand1"), functions.rand(142).as("rand2"),
      functions.randn().as("randn1"), functions.randn(42).as("randn2"),
      df("cylinder_number"))
    assert(df1.columns === Seq("rand1", "rand2", "randn1", "randn2", "cylinder_number"))
    assert(df1.schema("rand1").storageType === DoubleType)
    assert(df1.schema("rand2").storageType === DoubleType)
    assert(df1.schema("randn1").storageType === DoubleType)
    assert(df1.schema("randn2").storageType === DoubleType)
    assert(df1.numRows === df.numRows)
  }

  test("non-agg - spark_partition_id") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(functions.spark_partition_id().as("partition_id"), df("customer"))
    assert(df1.numRows === df.numRows)
    assert(df1.columns === Seq("partition_id", "customer"))
    assert(df1.schema("partition_id").storageType === IntegerType)
  }

  test("non-agg - struct") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(functions.struct("job_number", "grain_screened").as("struct_col"),
      df("job_number"), df("grain_screened"))
    assert(df1.numRows === df.numRows)
    assert(df1.columns === Seq("struct_col", "job_number", "grain_screened"))
    assert(df1.schema("struct_col").storageType.isInstanceOf[StructType])
    assert(df1.take(50).rows.forall {
      case Seq(v: Map[_, _], v1: Int, v2: String) =>
        v === Map("job_number" -> v1, "grain_screened" -> v2)
      case Seq(v: Map[_, _], null, v2: String) =>
        v === Map("job_number" -> null, "grain_screened" -> v2)
      case Seq(v: Map[_, _], v1: Int, null) =>
        v === Map("job_number" -> v1, "grain_screened" -> null)
      case Seq(v: Map[_, _], null, null) =>
        v === Map("job_number" -> null, "grain_screened" -> null)
    })

    val df2 = df.select(functions.struct(df("job_number"), df("grain_screened")).as("struct_col"),
      df("job_number"), df("grain_screened"))
    assert(df2.numRows === df.numRows)
    assert(df2.columns === Seq("struct_col", "job_number", "grain_screened"))
    assert(df2.schema("struct_col").storageType.isInstanceOf[StructType])
    assert(df2.take(50).rows.forall {
      case Seq(v: Map[_, _], v1: Int, v2: String) =>
        v === Map("job_number" -> v1, "grain_screened" -> v2)
      case Seq(v: Map[_, _], null, v2: String) =>
        v === Map("job_number" -> null, "grain_screened" -> v2)
      case Seq(v: Map[_, _], v1: Int, null) =>
        v === Map("job_number" -> v1, "grain_screened" -> null)
      case Seq(v: Map[_, _], null, null) =>
        v === Map("job_number" -> null, "grain_screened" -> null)
    })
  }

  test("non-agg - expr") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("customer"), functions.expr("length(customer)").as("len"))
    assert(df1.numRows === df.numRows)
    assert(df1.schema("len").storageType === IntegerType)
    assert(df1.take(50).rows.forall {
      case Seq(s: String, l: Int) => l === s.length
    })

    intercept[CebesBackendException] {
      df.select(functions.expr("wrong_function(abddddd)"))
    }
  }

  test("math - abs") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("hardener"), functions.rand().as("rand")).select(
      functions.col("hardener"), functions.col("rand"),
      functions.abs(functions.col("hardener") - functions.col("rand")).as("abs_col"))
    assert(df1.columns === Seq("hardener", "rand", "abs_col"))
    assert(df1.numRows === df.numRows)
    assert(df1.schema("abs_col").storageType === DoubleType)
    assert(df1.take(100).rows.forall {
      case Seq(v1: Float, v2: Double, v: Double) => v === math.abs(v1 - v2)
      case Seq(null, _: Double, null) => true
    })

    // on a string column
    intercept[CebesBackendException](df.select(functions.abs(df("customer"))))
  }

  test("math - sqrt") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("viscosity"), df("caliper"),
      functions.sqrt("viscosity").as("vis_sqrt"), functions.sqrt(df("caliper")).as("cal_sqrt"))
    assert(df1.numCols === 4)
    assert(df1.numRows === df.numRows)
    assert(df1.schema("vis_sqrt").storageType === DoubleType)
    assert(df1.schema("cal_sqrt").storageType === DoubleType)
    assert(df1.take(100).rows.forall {
      case Seq(v1: Int, v2: Float, v1s: Double, v2s: Double) =>
        math.sqrt(v1) === v1s && math.sqrt(v2) === v2s
      case Seq(null, v2: Float, null, v2s: Double) =>
        math.sqrt(v2) === v2s
      case Seq(v1: Int, null, v1s: Double, null) =>
        math.sqrt(v1) === v1s
      case Seq(null, null, null, null) => true
    })

    // on string column is fine too!
    val df2 = df.select(functions.sqrt("customer").as("sqrt_col"))
    assert(df2.schema("sqrt_col").storageType === DoubleType)
    assert(df2.take(50).get[Any]("sqrt_col").get.forall(_ === null))
  }

  test("math - bitwiseNot") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("viscosity"), functions.bitwiseNOT(df("viscosity")).as("vis_not"))
    assert(df1.numCols === 2)
    assert(df1.numRows === df.numRows)
    assert(df1.schema("vis_not").storageType === IntegerType)
    assert(df1.take(100).rows.forall {
      case Seq(v1: Int, v: Int) => v === ~v1
      case Seq(null, null) => true
    })

    // float column
    intercept[CebesBackendException](df.select(functions.bitwiseNOT(df("caliper"))))
  }

  test("math - acos") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(((functions.rand() * 2) - 1).as("rand")).select(
      functions.col("rand"),
      functions.acos("rand").as("acos1"),
      functions.acos(functions.col("rand")).as("acos2"))
    assert(df1.columns === Seq("rand", "acos1", "acos2"))
    assert(df1.schema("acos1").storageType === DoubleType)
    assert(df1.schema("acos2").storageType === DoubleType)
    assert(df1.take(50).rows.forall {
      case Seq(v: Double, ac1: Double, ac2: Double) =>
        ac1 === ac2 && ac1 === math.acos(v) && 0 <= ac1 && ac1 <= math.Pi
      case Seq(null, null, null) => true
    })

    // wrong domain
    val df2 = df.select(functions.acos("blade_pressure").as("acos_col"))
    assert(df2.schema("acos_col").storageType === DoubleType)

    // on string column is fine too!
    val df3 = df.select(functions.acos("customer").as("acos_col"))
    assert(df3.schema("acos_col").storageType === DoubleType)
    assert(df3.take(50).get[Any]("acos_col").get.forall(_ === null))
  }

  test("math - asin") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(((functions.rand() * 2) - 1).as("rand")).select(
      functions.col("rand"),
      functions.asin("rand").as("asin1"),
      functions.asin(functions.col("rand")).as("asin2"))
    assert(df1.columns === Seq("rand", "asin1", "asin2"))
    assert(df1.schema("asin1").storageType === DoubleType)
    assert(df1.schema("asin2").storageType === DoubleType)
    assert(df1.take(50).rows.forall {
      case Seq(v: Double, a1: Double, a2: Double) =>
        a1 === a2 && a1 === math.asin(v) && -math.Pi / 2 <= a1 && a1 <= math.Pi / 2
      case Seq(null, null, null) => true
    })

    // wrong domain
    val df2 = df.select(functions.asin("blade_pressure").as("asin_col"))
    assert(df2.schema("asin_col").storageType === DoubleType)

    // on string column is fine too!
    val df3 = df.select(functions.asin("customer").as("asin_col"))
    assert(df3.schema("asin_col").storageType === DoubleType)
    assert(df3.take(50).get[Any]("asin_col").get.forall(_ === null))
  }

  test("math - atan") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(
      functions.col("ink_temperature"),
      functions.atan("ink_temperature").as("atan1"),
      functions.atan(functions.col("ink_temperature")).as("atan2"))
    assert(df1.columns === Seq("ink_temperature", "atan1", "atan2"))
    assert(df1.schema("atan1").storageType === DoubleType)
    assert(df1.schema("atan2").storageType === DoubleType)
    assert(df1.take(50).rows.forall {
      case Seq(v: Float, a1: Double, a2: Double) =>
        a1 === a2 && a1 === math.atan(v) && -math.Pi / 2 <= a1 && a1 <= math.Pi / 2
      case Seq(null, null, null) => true
    })

    // on string column is fine too!
    val df3 = df.select(functions.atan("customer").as("atan_col"))
    assert(df3.schema("atan_col").storageType === DoubleType)
    assert(df3.take(50).get[Any]("atan_col").get.forall(_ === null))
  }

  test("math - atan2") {
    val df = getCylinderBands.limit(100)

    def test(xCol: => Column, yCol: => Column, f: => Column) = {
      val df1 = df.select(xCol.as("x"), yCol.as("y"), f.as("atan2_col"))
      assert(df1.columns === Seq("x", "y", "atan2_col"))
      assert(df1.numRows === df.numRows)
      assert(df1.schema("atan2_col").storageType === DoubleType)
      assert(df1.take(50).rows.forall {
        case Seq(x: Float, y: Float, v: Double) =>
          v === math.atan2(x, y)
        case Seq(null, _, null) => true
        case Seq(_, null, null) => true
        case Seq(null, null, null) => true
      })
    }

    test(df("caliper"), df("roughness"), functions.atan2("caliper", "roughness"))
    test(df("caliper"), df("roughness"), functions.atan2(df("caliper"), df("roughness")))
    test(df("caliper"), df("roughness"), functions.atan2(df("caliper"), "roughness"))
    test(df("caliper"), df("roughness"), functions.atan2("caliper", df("roughness")))
    test(df("caliper"), functions.lit(10.0f), functions.atan2(df("caliper"), 10))
    test(df("caliper"), functions.lit(10.0f), functions.atan2("caliper", 10))
    test(functions.lit(5.0f), df("roughness"), functions.atan2(5, df("roughness")))
    test(functions.lit(5.0f), df("roughness"), functions.atan2(5, "roughness"))
  }

  test("math - bin") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(
      functions.col("press"),
      functions.bin("press").as("bin1"),
      functions.bin(functions.col("press")).as("bin2"))
    assert(df1.columns === Seq("press", "bin1", "bin2"))
    assert(df1.schema("bin1").storageType === StringType)
    assert(df1.schema("bin2").storageType === StringType)
    assert(df1.take(50).rows.forall {
      case Seq(v: Int, s1: String, s2: String) =>
        s1 === s2 && s1 === v.toBinaryString
      case Seq(null, null, null) => true
    })

    // wrong data type (float)
    val df2 = df.select(functions.bin("roughness").as("bin_col"))
    assert(df2.schema("bin_col").storageType === StringType)

    // on string column is fine too!
    val df3 = df.select(functions.bin("customer").as("bin_col"))
    assert(df3.schema("bin_col").storageType === StringType)
    assert(df3.take(50).get[Any]("bin_col").get.forall(_ === null))
  }

  test("math - cbrt") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("viscosity"), df("caliper"),
      functions.cbrt("viscosity").as("vis_cbrt"), functions.cbrt(df("caliper")).as("cal_cbrt"))
    assert(df1.numCols === 4)
    assert(df1.numRows === df.numRows)
    assert(df1.schema("vis_cbrt").storageType === DoubleType)
    assert(df1.schema("cal_cbrt").storageType === DoubleType)
    assert(df1.take(100).rows.forall {
      case Seq(v1: Int, v2: Float, v1s: Double, v2s: Double) =>
        math.cbrt(v1) === v1s && math.cbrt(v2) === v2s
      case Seq(null, v2: Float, null, v2s: Double) => math.cbrt(v2) === v2s
      case Seq(v1: Int, null, v1s: Double, null) => math.cbrt(v1) === v1s
      case Seq(null, null, null, null) => true
    })

    // on string column is fine!
    val df2 = df.select(functions.cbrt("customer").as("cbrt_col"))
    assert(df2.take(100).get[Any]("cbrt_col").get.forall(_ === null))
  }

  test("math - ceil") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("viscosity"), df("caliper"),
      functions.ceil("viscosity").as("vis_ceil"), functions.ceil(df("caliper")).as("cal_ceil"))
    assert(df1.numCols === 4)
    assert(df1.numRows === df.numRows)
    assert(df1.schema("vis_ceil").storageType === LongType)
    assert(df1.schema("cal_ceil").storageType === LongType)
    assert(df1.take(100).rows.forall {
      case Seq(v1: Int, v2: Float, v1s: Long, v2s: Long) =>
        math.ceil(v1) === v1s && math.ceil(v2) === v2s
      case Seq(null, v2: Float, null, v2s: Long) => math.ceil(v2) === v2s
      case Seq(v1: Int, null, v1s: Long, null) => math.ceil(v1) === v1s
      case Seq(null, null, null, null) => true
    })

    // on string column is fine!
    val df2 = df.select(functions.ceil("customer").as("ceil_col"))
    assert(df2.take(100).get[Any]("ceil_col").get.forall(_ === null))
  }

  test("math - conv") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("press"),
      functions.conv(functions.bin(df("press")), 2, 8).as("conv8"),
      functions.conv(functions.bin(df("press")), 2, 9).as("conv9"))
    assert(df1.schema("conv8").storageType === StringType)
    assert(df1.schema("conv9").storageType === StringType)
    assert(df1.take(100).rows.forall {
      case Seq(v: Int, s1: String, s2: String) =>
        val v2 = BigInt(v)
        v2.toString(8) === s1 && v2.toString(9) === s2
      case Seq(null, null, null) => true
    })

    val df2 = df.select(functions.conv(df("customer"), 2, 10).as("conv"))
    assert(df2.take(50).get[Any]("conv").get.forall(_ === "0"))
  }

  test("math - cos") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("varnish_pct"),
      functions.cos("varnish_pct").as("cos1"),
      functions.cos(df("varnish_pct")).as("cos2"))
    assert(df1.columns === Seq("varnish_pct", "cos1", "cos2"))
    assert(df1.schema("cos1").storageType === DoubleType)
    assert(df1.schema("cos2").storageType === DoubleType)
    assert(df1.take(50).rows.forall {
      case Seq(v: Float, v1: Double, v2: Double) =>
        v1 === v2 && math.cos(v) === v1 && -1 <= v1 && v1 <= 1
      case Seq(null, null, null) => true
    })

    // on string column is fine too!
    val df3 = df.select(functions.cos("customer").as("cos_col"))
    assert(df3.schema("cos_col").storageType === DoubleType)
    assert(df3.take(50).get[Any]("cos_col").get.forall(_ === null))
  }

  test("math - cosh") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("varnish_pct"),
      functions.cosh("varnish_pct").as("cosh1"),
      functions.cosh(df("varnish_pct")).as("cosh2"))
    assert(df1.columns === Seq("varnish_pct", "cosh1", "cosh2"))
    assert(df1.schema("cosh1").storageType === DoubleType)
    assert(df1.schema("cosh2").storageType === DoubleType)
    assert(df1.take(50).rows.forall {
      case Seq(v: Float, v1: Double, v2: Double) =>
        v1 === v2 && math.cosh(v) === v1 && 1 <= v1
      case Seq(null, null, null) => true
    })

    // on string column is fine too!
    val df3 = df.select(functions.cos("customer").as("cosh_col"))
    assert(df3.schema("cosh_col").storageType === DoubleType)
    assert(df3.take(50).get[Any]("cosh_col").get.forall(_ === null))
  }

  test("math - exp") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("roughness"),
      functions.exp("roughness").as("exp1"),
      functions.exp(df("roughness")).as("exp2"))
    assert(df1.columns === Seq("roughness", "exp1", "exp2"))
    assert(df1.schema("exp1").storageType === DoubleType)
    assert(df1.schema("exp2").storageType === DoubleType)
    assert(df1.take(50).rows.forall {
      case Seq(v: Float, v1: Double, v2: Double) =>
        v1 === v2 && math.exp(v) === v1 && 0 <= v1
      case Seq(null, null, null) => true
    })

    // on string column is fine too!
    val df3 = df.select(functions.exp("customer").as("exp_col"))
    assert(df3.schema("exp_col").storageType === DoubleType)
    assert(df3.take(50).get[Any]("exp_col").get.forall(_ === null))
  }

  test("math - expm1") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("roughness"),
      functions.expm1("roughness").as("expm1"),
      functions.expm1(df("roughness")).as("expm2"))
    assert(df1.columns === Seq("roughness", "expm1", "expm2"))
    assert(df1.schema("expm1").storageType === DoubleType)
    assert(df1.schema("expm2").storageType === DoubleType)
    assert(df1.take(50).rows.forall {
      case Seq(v: Float, v1: Double, v2: Double) =>
        v1 === v2 && math.expm1(v) === v1 && 0 <= v1
      case Seq(null, null, null) => true
    })

    // on string column is fine too!
    val df3 = df.select(functions.expm1("customer").as("expm_col"))
    assert(df3.schema("expm_col").storageType === DoubleType)
    assert(df3.take(50).get[Any]("expm_col").get.forall(_ === null))
  }

  test("math - factorial") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("viscosity"), functions.factorial(df("viscosity").mod(10)).as("fact"))
    assert(df1.schema("fact").storageType === LongType)
    assert(df1.take(50).rows.forall {
      case Seq(v: Int, v1: Long) => v1 === (1 to (v % 10)).par.product
      case Seq(null, null) => true
    })

    // on string column is fine too!
    val df3 = df.select(functions.factorial(df("customer")).as("fact_col"))
    assert(df3.schema("fact_col").storageType === LongType)
    assert(df3.take(50).get[Any]("fact_col").get.forall(_ === null))
  }

  test("math - floor") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("viscosity"), df("caliper"),
      functions.floor("viscosity").as("vis_floor"), functions.floor(df("caliper")).as("cal_floor"))
    assert(df1.numCols === 4)
    assert(df1.numRows === df.numRows)
    assert(df1.schema("vis_floor").storageType === LongType)
    assert(df1.schema("cal_floor").storageType === LongType)
    assert(df1.take(100).rows.forall {
      case Seq(v1: Int, v2: Float, v1s: Long, v2s: Long) =>
        math.floor(v1) === v1s && math.floor(v2) === v2s
      case Seq(null, v2: Float, null, v2s: Long) => math.floor(v2) === v2s
      case Seq(v1: Int, null, v1s: Long, null) => math.floor(v1) === v1s
      case Seq(null, null, null, null) => true
    })

    // on string column is fine!
    val df2 = df.select(functions.floor("customer").as("floor_col"))
    assert(df2.take(100).get[Any]("floor_col").get.forall(_ === null))
  }

  test("math - greatest") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("viscosity"), df("caliper"),
      functions.greatest("viscosity", "caliper").as("greatest1"),
      functions.greatest(df("viscosity"), df("caliper")).as("greatest2"))
    assert(df1.numCols === 4)
    assert(df1.numRows === df.numRows)
    assert(df1.schema("greatest1").storageType === FloatType)
    assert(df1.schema("greatest2").storageType === FloatType)
    assert(df1.take(100).rows.forall {
      case Seq(v1: Int, v2: Float, v1s: Float, v2s: Float) =>
        v1s === v2s && v1s === math.max(v1, v2)
      case Seq(null, v2: Float, v1s: Float, v2s: Float) => v2 === v1s && v1s === v2s
      case Seq(v1: Int, null, v1s: Float, v2s: Float) => v1 === v1s && v1s === v2s
      case Seq(null, null, null, null) => true
    })

    // on string column is fine!
    val df2 = df.select(df("customer"), df("type_on_cylinder"),
      functions.greatest("customer", "type_on_cylinder").as("greatest_col"))
    assert(df2.schema("greatest_col").storageType === StringType)
    assert(df2.take(50).rows.forall {
      case Seq(c: String, t: String, v: String) => v === (if (c > t) c else t)
      case Seq(null, t: String, v: String) => v === t
      case Seq(c: String, null, v: String) => v === c
      case Seq(null, null, null) => true
    })

    intercept[IllegalArgumentException](df.select(functions.greatest("customer")))
  }

  test("math - hex") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(functions.hex(df("customer")).as("hex1"),
      functions.hex(df("plating_tank")).as("hex2"))
    assert(df1.schema("hex1").storageType === StringType)
    assert(df1.schema("hex2").storageType === StringType)
  }

  test("math - unhex") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(functions.unhex(functions.hex(df("customer"))).as("unhex1"))
    assert(df1.schema("unhex1").storageType === BinaryType)
    assert(df1.take(50).get[Array[Byte]]("unhex1").get.forall(_.length >= 0))
  }

  test("math - hypot") {
    val df = getCylinderBands.limit(100)

    def test(xCol: => Column, yCol: => Column, f: => Column) = {
      val df1 = df.select(xCol.as("x"), yCol.as("y"), f.as("hypot_col"))
      assert(df1.columns === Seq("x", "y", "hypot_col"))
      assert(df1.numRows === df.numRows)
      assert(df1.schema("hypot_col").storageType === DoubleType)
      assert(df1.take(50).rows.forall {
        case Seq(x: Float, y: Float, v: Double) =>
          v === math.hypot(x, y)
        case Seq(null, _, null) => true
        case Seq(_, null, null) => true
        case Seq(null, null, null) => true
      })
    }

    test(df("caliper"), df("roughness"), functions.hypot("caliper", "roughness"))
    test(df("caliper"), df("roughness"), functions.hypot(df("caliper"), df("roughness")))
    test(df("caliper"), df("roughness"), functions.hypot(df("caliper"), "roughness"))
    test(df("caliper"), df("roughness"), functions.hypot("caliper", df("roughness")))
    test(df("caliper"), functions.lit(10.0f), functions.hypot(df("caliper"), 10))
    test(df("caliper"), functions.lit(10.0f), functions.hypot("caliper", 10))
    test(functions.lit(5.0f), df("roughness"), functions.hypot(5, df("roughness")))
    test(functions.lit(5.0f), df("roughness"), functions.hypot(5, "roughness"))
  }

  test("math - least") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("viscosity"), df("caliper"),
      functions.least("viscosity", "caliper").as("least1"),
      functions.least(df("viscosity"), df("caliper")).as("least2"))
    assert(df1.numCols === 4)
    assert(df1.numRows === df.numRows)
    assert(df1.schema("least1").storageType === FloatType)
    assert(df1.schema("least2").storageType === FloatType)
    assert(df1.take(100).rows.forall {
      case Seq(v1: Int, v2: Float, v1s: Float, v2s: Float) =>
        v1s === v2s && v1s === math.min(v1, v2)
      case Seq(null, v2: Float, v1s: Float, v2s: Float) => v2 === v1s && v1s === v2s
      case Seq(v1: Int, null, v1s: Float, v2s: Float) => v1 === v1s && v1s === v2s
      case Seq(null, null, null, null) => true
    })

    // on string column is fine!
    val df2 = df.select(df("customer"), df("type_on_cylinder"),
      functions.least("customer", "type_on_cylinder").as("least_col"))
    assert(df2.schema("least_col").storageType === StringType)
    assert(df2.take(50).rows.forall {
      case Seq(c: String, t: String, v: String) => v === (if (c < t) c else t)
      case Seq(null, t: String, v: String) => v === t
      case Seq(c: String, null, v: String) => v === c
      case Seq(null, null, null) => true
    })

    intercept[IllegalArgumentException](df.select(functions.least("customer")))
  }

  test("math - log") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("ink_pct"),
      functions.log("ink_pct").as("loge1"), functions.log(df("ink_pct")).as("loge2"),
      functions.log(3, "ink_pct").as("log31"), functions.log(3, df("ink_pct")).as("log32"),
      functions.log1p("ink_pct").as("log1p1"), functions.log1p(df("ink_pct")).as("log1p2"),
      functions.log2("ink_pct").as("log21"), functions.log2(df("ink_pct")).as("log22"),
      functions.log10("ink_pct").as("log101"), functions.log10(df("ink_pct")).as("log102"))
    assert(Seq("loge1", "loge2", "log31", "log32", "log1p1", "log1p2",
      "log21", "log22", "log101", "log102").forall(s => df1.schema(s).storageType === DoubleType))
    assert(df1.take(50).rows.forall {
      case Seq(v: Float, ve1: Double, ve2: Double, v31: Double, v32: Double,
      v1p1: Double, v1p2: Double, v21: Double, v22: Double, v101: Double, v102: Double) =>
        math.log(v) === ve1 && ve1 === ve2 &&
          (math.log(v) / math.log(3)) === v31 && v31 === v32 &&
          math.log1p(v) === v1p1 && v1p1 === v1p2 &&
          math.log(v) / math.log(2) === v21 && v21 === v22 &&
          math.log10(v) === v101 && v101 === v102
      case s: Seq[_] => s.forall(_ === null)
    })

    val df2 = df.select(functions.log(df("customer")).as("log_col"))
    assert(df2.schema("log_col").storageType === DoubleType)
    assert(df2.take(50).get[Any]("log_col").get.forall(_ === null))
  }

  test("math - pow") {
    val df = getCylinderBands.limit(100)

    def test(xCol: => Column, yCol: => Column, f: => Column) = {
      val df1 = df.select(xCol.as("x"), yCol.as("y"), f.as("pow_col"))
      assert(df1.columns === Seq("x", "y", "pow_col"))
      assert(df1.numRows === df.numRows)
      assert(df1.schema("pow_col").storageType === DoubleType)
      assert(df1.take(50).rows.forall {
        case Seq(x: Float, y: Float, v: Double) =>
          math.abs(v - math.pow(x, y)) <= 1e-6
        case Seq(null, _, null) => true
        case Seq(_, null, null) => true
        case Seq(null, null, null) => true
      })
    }

    test(df("caliper"), df("roughness"), functions.pow("caliper", "roughness"))
    test(df("caliper"), df("roughness"), functions.pow(df("caliper"), df("roughness")))
    test(df("caliper"), df("roughness"), functions.pow(df("caliper"), "roughness"))
    test(df("caliper"), df("roughness"), functions.pow("caliper", df("roughness")))
    test(df("caliper"), functions.lit(1.2f), functions.pow(df("caliper"), 1.2))
    test(df("caliper"), functions.lit(1.2f), functions.pow("caliper", 1.2))
    test(functions.lit(1.5f), df("roughness"), functions.pow(1.5, df("roughness")))
    test(functions.lit(1.5f), df("roughness"), functions.pow(1.5, "roughness"))
  }

  test("math - pmod") {
    val df = getCylinderBands.limit(100)

    def pmod(x: Int, y: Int) = {
      val r = x % y
      if (r < 0) r + y else r
    }

    val df1 = df.select(df("plating_tank"), df("viscosity"),
      functions.pmod(df("plating_tank"), df("viscosity")).as("pmod1"),
      functions.pmod(df("plating_tank"), -df("viscosity")).as("pmod2"))
    assert(df1.schema("pmod1").storageType === IntegerType)
    assert(df1.schema("pmod2").storageType === IntegerType)
    assert(df1.take(50).rows.forall {
      case Seq(p: Int, v: Int, m1: Int, m2: Int) =>
        m1 === pmod(p, v) && m2 === pmod(p, -v)
      case Seq(null, _, null, null) => true
      case Seq(_, null, null, null) => true
      case Seq(null, null, null, null) => true
    })

    val df2 = df.select(df("plating_tank"), df("caliper"),
      functions.pmod(df("plating_tank"), df("caliper")).as("pmod"))
    assert(df2.schema("pmod").storageType === FloatType)
  }

  test("math - rint") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("ink_pct"),
      functions.rint("ink_pct").as("rint1"),
      functions.rint(df("ink_pct")).as("rint2"))
    assert(df1.columns === Seq("ink_pct", "rint1", "rint2"))
    assert(df1.schema("rint1").storageType === DoubleType)
    assert(df1.schema("rint2").storageType === DoubleType)
    assert(df1.take(50).rows.forall {
      case Seq(v: Float, v1: Double, v2: Double) =>
        v1 === v2 && math.rint(v) === v1
      case Seq(null, null, null) => true
    })

    // on string column is fine too!
    val df3 = df.select(functions.rint("customer").as("rint_col"))
    assert(df3.schema("rint_col").storageType === DoubleType)
    assert(df3.take(50).get[Any]("rint_col").get.forall(_ === null))
  }

  test("math - round") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("ink_pct"),
      functions.round(df("ink_pct")).as("round1"),
      functions.round(df("ink_pct"), 2).as("round2"),
      functions.round(df("ink_pct"), -2).as("round3"))
    assert(df1.columns === Seq("ink_pct", "round1", "round2", "round3"))
    assert(df1.schema("round1").storageType === FloatType)
    assert(df1.schema("round2").storageType === FloatType)
    assert(df1.schema("round3").storageType === FloatType)
    assert(df1.take(50).rows.forall {
      case Seq(v: Float, v1: Float, v2: Float, v3: Float) =>
        Seq(v1, v2, v3).forall(u => math.abs(v - u) <= 0.5)
      case Seq(null, null, null, null) => true
    })

    // on string column is fine too!
    val df3 = df.select(functions.round(df("customer")).as("round_col"))
    assert(df3.schema("round_col").storageType === DoubleType)
    assert(df3.take(50).get[Any]("round_col").get.forall(_ === null))
  }

  test("math - bround") {
    val df = getCylinderBands.limit(100)

    def bround(v: Float, p: Int): Float =
      BigDecimal(v).setScale(p, BigDecimal.RoundingMode.HALF_EVEN).toFloat

    val df1 = df.select(df("ink_pct"),
      functions.bround(df("ink_pct")).as("bround1"),
      functions.bround(df("ink_pct"), 2).as("bround2"),
      functions.bround(df("ink_pct"), -2).as("bround3"))
    assert(df1.columns === Seq("ink_pct", "bround1", "bround2", "bround3"))
    assert(df1.schema("bround1").storageType === FloatType)
    assert(df1.schema("bround2").storageType === FloatType)
    assert(df1.schema("bround3").storageType === FloatType)
    assert(df1.take(50).rows.forall {
      case Seq(v: Float, v1: Float, v2: Float, v3: Float) =>
        v1 === bround(v, 0) && v2 === bround(v, 2) && v3 === bround(v, -2)
      case Seq(null, null, null, null) => true
    })

    // on string column is fine too!
    val df3 = df.select(functions.bround(df("customer")).as("bround_col"))
    assert(df3.schema("bround_col").storageType === DoubleType)
    assert(df3.take(50).get[Any]("bround_col").get.forall(_ === null))
  }

  test("math - shift") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("press"),
      functions.shiftLeft(df("press"), 2).as("shift1"),
      functions.shiftRight(df("press"), 2).as("shift2"),
      functions.shiftRightUnsigned(df("press"), 2).as("shift3"))
    assert(df1.columns === Seq("press", "shift1", "shift2", "shift3"))
    assert(df1.schema("shift1").storageType === IntegerType)
    assert(df1.schema("shift2").storageType === IntegerType)
    assert(df1.schema("shift3").storageType === IntegerType)
    assert(df1.take(50).rows.forall {
      case Seq(v: Int, v1: Int, v2: Int, v3: Int) =>
        v1 === (v << 2) && v2 === (v >> 2) && v3 === (v >>> 2)
      case Seq(null, null, null, null) => true
    })

    // on string column is fine too!
    val df3 = df.select(functions.shiftLeft(df("customer"), 2).as("shift_col"))
    assert(df3.schema("shift_col").storageType === IntegerType)
    assert(df3.take(50).get[Any]("shift_col").get.forall(_ === null))
  }

  test("math - signum") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("ink_pct"),
      functions.signum("ink_pct").as("signum1"),
      functions.signum(df("ink_pct")).as("signum2"))
    assert(df1.columns === Seq("ink_pct", "signum1", "signum2"))
    assert(df1.schema("signum1").storageType === DoubleType)
    assert(df1.schema("signum2").storageType === DoubleType)
    assert(df1.take(50).rows.forall {
      case Seq(v: Float, v1: Double, v2: Double) =>
        v1 == math.signum(v) && v2 === v1
      case Seq(null, null, null) => true
    })

    // on string column is fine too!
    val df3 = df.select(functions.signum("customer").as("signum_col"))
    assert(df3.schema("signum_col").storageType === DoubleType)
    assert(df3.take(50).get[Any]("signum_col").get.forall(_ === null))
  }

  test("math - sin") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("varnish_pct"),
      functions.sin("varnish_pct").as("sin1"),
      functions.sin(df("varnish_pct")).as("sin2"))
    assert(df1.columns === Seq("varnish_pct", "sin1", "sin2"))
    assert(df1.schema("sin1").storageType === DoubleType)
    assert(df1.schema("sin2").storageType === DoubleType)
    assert(df1.take(50).rows.forall {
      case Seq(v: Float, v1: Double, v2: Double) =>
        v1 === v2 && math.sin(v) === v1 && -1 <= v1 && v1 <= 1
      case Seq(null, null, null) => true
    })

    // on string column is fine too!
    val df3 = df.select(functions.sin("customer").as("sin_col"))
    assert(df3.schema("sin_col").storageType === DoubleType)
    assert(df3.take(50).get[Any]("sin_col").get.forall(_ === null))
  }

  test("math - sinh") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("varnish_pct"),
      functions.sinh("varnish_pct").as("sinh1"),
      functions.sinh(df("varnish_pct")).as("sinh2"))
    assert(df1.columns === Seq("varnish_pct", "sinh1", "sinh2"))
    assert(df1.schema("sinh1").storageType === DoubleType)
    assert(df1.schema("sinh2").storageType === DoubleType)
    assert(df1.take(50).rows.forall {
      case Seq(v: Float, v1: Double, v2: Double) =>
        v1 === v2 && math.sinh(v) === v1 && v1 * v >= 0
      case Seq(null, null, null) => true
    })

    // on string column is fine too!
    val df3 = df.select(functions.sinh("customer").as("sinh_col"))
    assert(df3.schema("sinh_col").storageType === DoubleType)
    assert(df3.take(50).get[Any]("sinh_col").get.forall(_ === null))
  }

  test("math - tan") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("varnish_pct"),
      functions.tan("varnish_pct").as("tan1"),
      functions.tan(df("varnish_pct")).as("tan2"))
    assert(df1.columns === Seq("varnish_pct", "tan1", "tan2"))
    assert(df1.schema("tan1").storageType === DoubleType)
    assert(df1.schema("tan2").storageType === DoubleType)
    assert(df1.take(50).rows.forall {
      case Seq(v: Float, v1: Double, v2: Double) =>
        v1 === v2 && math.tan(v) === v1
      case Seq(null, null, null) => true
    })

    // on string column is fine too!
    val df3 = df.select(functions.tan("customer").as("tan_col"))
    assert(df3.schema("tan_col").storageType === DoubleType)
    assert(df3.take(50).get[Any]("tan_col").get.forall(_ === null))
  }

  test("math - tanh") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("varnish_pct"),
      functions.tanh("varnish_pct").as("tanh1"),
      functions.tanh(df("varnish_pct")).as("tanh2"))
    assert(df1.columns === Seq("varnish_pct", "tanh1", "tanh2"))
    assert(df1.schema("tanh1").storageType === DoubleType)
    assert(df1.schema("tanh2").storageType === DoubleType)
    assert(df1.take(50).rows.forall {
      case Seq(v: Float, v1: Double, v2: Double) =>
        v1 === v2 && math.tanh(v) === v1 && v1 * v >= 0
      case Seq(null, null, null) => true
    })

    // on string column is fine too!
    val df3 = df.select(functions.tanh("customer").as("tan_col"))
    assert(df3.schema("tan_col").storageType === DoubleType)
    assert(df3.take(50).get[Any]("tan_col").get.forall(_ === null))
  }

  test("math - toDegrees") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("hardener"),
      functions.toDegrees("hardener").as("d1"),
      functions.toDegrees(df("hardener")).as("d2"))
    assert(df1.columns === Seq("hardener", "d1", "d2"))
    assert(df1.schema("d1").storageType === DoubleType)
    assert(df1.schema("d2").storageType === DoubleType)
    assert(df1.take(50).rows.forall {
      case Seq(v: Float, v1: Double, v2: Double) =>
        v1 === v2 && math.toDegrees(v) === v1
      case Seq(null, null, null) => true
    })

    // on string column is fine too!
    val df3 = df.select(functions.toDegrees("customer").as("d_col"))
    assert(df3.schema("d_col").storageType === DoubleType)
    assert(df3.take(50).get[Any]("d_col").get.forall(_ === null))
  }

  test("math - toRadians") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("ink_pct"),
      functions.toRadians("ink_pct").as("r1"),
      functions.toRadians(df("ink_pct")).as("r2"))
    assert(df1.columns === Seq("ink_pct", "r1", "r2"))
    assert(df1.schema("r1").storageType === DoubleType)
    assert(df1.schema("r2").storageType === DoubleType)
    assert(df1.take(50).rows.forall {
      case Seq(v: Float, v1: Double, v2: Double) =>
        v1 === v2 && math.toRadians(v) === v1
      case Seq(null, null, null) => true
    })

    // on string column is fine too!
    val df3 = df.select(functions.toRadians("customer").as("r_col"))
    assert(df3.schema("r_col").storageType === DoubleType)
    assert(df3.take(50).get[Any]("r_col").get.forall(_ === null))
  }

  ////////////////////////////////////////////////////////////////////////////////////
  // Misc functions
  ////////////////////////////////////////////////////////////////////////////////////

  test("misc - md5 and sha1") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("customer").cast(BinaryType).as("bin_col")).select(functions.col("bin_col"),
      functions.md5(functions.col("bin_col")).as("md5"),
      functions.sha1(functions.col("bin_col")).as("sha1"))
    assert(df1.columns === Seq("bin_col", "md5", "sha1"))
    assert(df1.schema("md5").storageType === StringType)
    assert(df1.schema("sha1").storageType === StringType)
    assert(df1.take(50).rows.forall {
      case Seq(_: Array[Byte], v1: String, v2: String) =>
        v1.length === 32 && v2.length === 40
      case Seq(null, null, null) => true
    })

    // not binary column
    intercept[CebesBackendException](df.select(functions.sha1(df("ink_pct"))))
  }

  test("misc - sha2") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("customer").cast(BinaryType).as("bin_col")).select(
      functions.col("bin_col"),
      functions.sha2(functions.col("bin_col"), 384).as("sha384"),
      functions.sha2(functions.col("bin_col"), 512).as("sha512"))
    assert(df1.columns === Seq("bin_col", "sha384", "sha512"))
    assert(df1.schema("sha384").storageType === StringType)
    assert(df1.schema("sha512").storageType === StringType)
    assert(df1.take(50).rows.forall {
      case Seq(_: Array[Byte], v1: String, v2: String) =>
        v1.length === 96 && v2.length === 128
      case Seq(null, null, null) => true
    })

    // not binary column
    intercept[CebesBackendException](df.select(functions.sha2(df("ink_pct"), 384)))

    // invalid numBits
    intercept[IllegalArgumentException] {
      df.select(df("customer").cast(BinaryType).as("bin_col")).select(
        functions.sha2(functions.col("bin_col"), 199).as("sha384"))
    }
  }

  test("misc - crc32 and hash") {
    val df = getCylinderBands.limit(100)

    val df1 = df.select(df("customer").cast(BinaryType).as("bin_col"), df("customer")).select(
      functions.col("bin_col"),
      functions.crc32(functions.col("bin_col")).as("crc32"),
      functions.hash(functions.col("bin_col"), functions.col("customer")).as("hash"))
    assert(df1.columns === Seq("bin_col", "crc32", "hash"))
    assert(df1.schema("crc32").storageType === LongType)
    assert(df1.schema("hash").storageType === IntegerType)
    assert(df1.take(50).rows.forall {
      case Seq(_: Array[Byte], v1: Long, v2: Int) =>
        v1.isValidLong && Int.MinValue < v2 && v2 < Int.MaxValue
      case Seq(null, null, null) => true
    })

    // not binary column
    intercept[CebesBackendException](df.select(functions.crc32(df("ink_pct"))))
  }

  ////////////////////////////////////////////////////////////////////////////////////
  // String functions
  // TODO: add tests
  ////////////////////////////////////////////////////////////////////////////////////


  ////////////////////////////////////////////////////////////////////////////////////
  // Datetime functions
  // TODO: add tests
  ////////////////////////////////////////////////////////////////////////////////////

  test("datetime - window") {
    val df = getCylinderBands.limit(100)
    val df1 = df.select(df("timestamp").cast(StorageTypes.TimestampType), df("press"), df("customer"))
    assert(df1.columns === Seq("timestamp", "press", "customer"))
    assert(df1.schema("timestamp").storageType === StorageTypes.TimestampType)
    val sample1 = df1.take(10)
    sample1("timestamp").forall { v =>
      val opt = Option(v)
      opt.isEmpty || opt.get.isInstanceOf[Timestamp]
    }

    val df2 = df1.groupBy(functions.window(df1("timestamp"), "5 seconds").as("window_col"))
      .agg(functions.avg("press").as("avg_press"))
    assert(df2.columns === Seq("window_col", "avg_press"))
    assert(df2.schema("window_col").storageType.isInstanceOf[StructType])
    val sample2 = df2.take(10)
    sample2("window_col").forall { v =>
      Option(v) match {
        case None => true
        case Some(m: Map[_, _]) =>
          val m2 = m.asInstanceOf[Map[String, Timestamp]]
          m2.size === 2 && m2.contains("start") && m2.contains("end")
        case _ => false
      }
    }
  }

  ////////////////////////////////////////////////////////////////////////////////////
  // Collection functions
  // TODO: add tests
  ////////////////////////////////////////////////////////////////////////////////////

}
