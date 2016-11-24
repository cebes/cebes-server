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

import io.cebes.common.CebesBackendException
import io.cebes.df.types.storage.{DoubleType, IntegerType, LongType}
import io.cebes.df.types.{StorageTypes, VariableTypes}
import io.cebes.spark.helpers.{TestDataHelper, TestPropertyHelper}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import io.cebes.df.functions

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

    intercept[CebesBackendException]{
      df.sort(df.col("timestampZXXXXX"))
    }
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
    assert(sample.rows.forall{ s =>
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
    // TODO: implement
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
    println("RollUp:\n" + df1.take(10).tabulate())
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
    println("Cube:\n" + df1.take(10).tabulate())
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
      functions.approxCountDistinct("customer", rsd=0.05))
    assert(df1.numCols === 2)
    assert(df1.numRows === 1)

    val df2 = df.agg(functions.approxCountDistinct(df("customer")),
      functions.approxCountDistinct(df("customer"), rsd=0.05))
    assert(df2.numCols === 2)
    assert(df2.numRows === 1)

    // rsd is too high
    intercept[IllegalArgumentException] {
      df.agg(functions.approxCountDistinct("customer", rsd=0.5))
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
  }

  test("agg - collectSet") {
    val df = getCylinderBands.limit(30)

    val df1 = df.agg(functions.collectSet("customer"),
      functions.collectSet(df("job_number") / 2))
    assert(df1.numCols === 2)
    assert(df1.numRows === 1)
  }


}
