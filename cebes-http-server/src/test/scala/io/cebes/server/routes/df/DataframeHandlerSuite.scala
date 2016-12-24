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
 * Created by phvu on 13/12/2016.
 */

package io.cebes.server.routes.df

import java.util.UUID

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import io.cebes.df.expressions._
import io.cebes.df.sample.DataSample
import io.cebes.df.types.StorageTypes
import io.cebes.df.{Column, functions}
import io.cebes.server.client.{RemoteDataframe, ServerException}
import io.cebes.server.routes.AbstractRouteSuite
import io.cebes.server.routes.df.HttpDfJsonProtocol._
import org.scalatest.BeforeAndAfterAll

class DataframeHandlerSuite extends AbstractRouteSuite with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    super.beforeAll()
    createOrReplaceCylinderBands()
  }

  /**
    * Private helper for taking a sample of the given dataframe
    */
  private def take(df: RemoteDataframe, n: Int): DataSample = {
    val sample = wait[DataSample](postAsync[LimitRequest, DataSample]("df/take", LimitRequest(df.id, n)))
    assert(sample.schema === df.schema)
    assert(sample.data.length === sample.schema.size)
    assert(sample.data.forall(_.length === n))
    sample
  }

  /**
    * Private helper for counting number of rows in the given dataframe
    */
  private def count(df: RemoteDataframe): Long = {
    wait(postAsync[DataframeRequest, Long]("df/count", DataframeRequest(df.id)))
  }

  /////////////////////////////////////////////
  // tests
  /////////////////////////////////////////////

  test("count") {
    val df = getCylinderBands

    val rows = wait[Long](postAsync[DataframeRequest, Long]("df/count", DataframeRequest(df.id)))
    assert(rows === 540)

    val ex = intercept[ServerException] {
      wait[Long](postAsync[DataframeRequest, Long]("df/count", DataframeRequest(UUID.randomUUID())))
    }
    assert(ex.message.startsWith("Dataframe ID not found"))
  }

  test("take") {
    val df = getCylinderBands

    val sample = wait[DataSample](postAsync[LimitRequest, DataSample]("df/take", LimitRequest(df.id, 15)))
    assert(sample.schema === df.schema)
    assert(sample.data.length === sample.schema.size)
    assert(sample.data.forall(_.length === 15))

    val ex1 = intercept[ServerException] {
      wait[DataSample](postAsync[LimitRequest, DataSample]("df/take", LimitRequest(df.id, -15)))
    }
    assert(ex1.getMessage.startsWith("The limit expression must be equal to or greater than 0"))

    val ex2 = intercept[ServerException] {
      wait[DataSample](postAsync[LimitRequest, DataSample]("df/take", LimitRequest(UUID.randomUUID(), 15)))
    }
    assert(ex2.message.startsWith("Dataframe ID not found"))
  }

  test("sample") {
    val df = getCylinderBands

    val df2 = waitDf("df/sample", SampleRequest(df.id, withReplacement = true, 0.5, 42))
    assert(df2.schema === df.schema)
    assert(df2.id !== df.id)

    val ex = intercept[ServerException] {
      waitDf("df/sample", SampleRequest(UUID.randomUUID(), withReplacement = true, 0.5, 42))
    }
    assert(ex.message.startsWith("Dataframe ID not found"))
  }

  test("sort") {
    val df = getCylinderBands

    val df1 = waitDf("df/sort", ColumnsRequest(df.id, Array(df.col("timestamp").desc, df.col("customer").asc)))
    assert(df1.schema === df.schema)

    val sample1 = take(df1, 50)
    val colTimestamp = sample1.get[Long]("timestamp").get
    assert(colTimestamp.zip(colTimestamp.tail).forall(t => t._1 >= t._2))

    val ex = intercept[ServerException] {
      waitDf("df/sort", ColumnsRequest(df.id, Array(df.col("non_existed_column").asc)))
    }
    assert(ex.getMessage.startsWith("Spark query analysis exception: Cannot resolve column name"))
  }

  test("drop") {
    val df = getCylinderBands

    val df1 = waitDf("df/dropcolumns", ColumnNamesRequest(df.id, Array("TimeStamp", "ink_cOlor", "wrong_column")))
    assert(df1.schema.size === df.schema.size - 2)
    assert(df1.schema.get("timestamp").isEmpty)
    assert(df1.schema.get("ink_cOlor").isEmpty)

    val df2 = waitDf("df/dropcolumns", ColumnNamesRequest(df.id, Array()))
    assert(df2.schema.size === df.schema.size)
    assert(df2.schema.get("timestamp").nonEmpty)
    assert(df2.schema.get("ink_cOlor").nonEmpty)
    assert(df2.schema === df.schema)
  }

  test("dropDuplicates") {
    val df = getCylinderBands

    val df1 = waitDf("df/dropduplicates", ColumnNamesRequest(df.id, Array("TimeStamp", "ink_cOlor")))
    assert(df1.schema === df.schema)

    val ex1 = intercept[ServerException] {
      waitDf("df/dropduplicates", ColumnNamesRequest(df.id, Array("TimeStamp", "ink_cOlor", "wrong_column")))
    }
    assert(ex1.getMessage.startsWith("Spark query analysis exception: Cannot resolve column name \"wrong_column\""))

    val df2 = waitDf("df/dropcolumns", ColumnNamesRequest(df.id, Array()))
    assert(df2.schema === df.schema)
  }

  ////////////////////
  // NA functions
  ////////////////////

  test("DropNA") {
    val df = getCylinderBands
    assert(count(df) === 540)

    // any, on all columns
    val df1 = waitDf("df/dropna", DropNARequest(df.id, df.schema.fields.length, df.schema.fieldNames.toArray))
    assert(df1.schema === df.schema)
    assert(count(df1) === 357)

    // all, on all columns
    val df2 = waitDf("df/dropna", DropNARequest(df.id, 1, df.schema.fieldNames.toArray))
    assert(count(df2) === count(df))

    // any, on some columns
    val df3 = waitDf("df/dropna", DropNARequest(df.id, 2, Array("plating_tank", "grain_screened")))
    assert(count(df3) === 522)

    intercept[ServerException](waitDf("df/dropna", DropNARequest(df.id, 1, Array("non_existed_column"))))

    val df6 = waitDf("df/dropna", DropNARequest(df.id, 1, Array("plating_tank", "proof_cut")))
    assert(count(df6) === 535)

    // minNonNull on some columns
    val df8 = waitDf("df/dropna", DropNARequest(df.id, 2, Array("grain_screened", "plating_tank", "proof_cut")))
    assert(count(df8) === 535)
  }

  test("fillNA") {
    val df = getCylinderBands

    def dropNA(df: RemoteDataframe, cols: Seq[String]): RemoteDataframe = {
      waitDf("df/dropna", DropNARequest(df.id, cols.length, cols.toArray))
    }

    val df1 = waitDf("df/fillna", FillNARequest(df.id, Right(0.2), df.schema.fieldNames.toArray))
    assert(count(dropNA(df1, df1.schema.fieldNames)) === count(df))

    val df2 = waitDf("df/fillna", FillNARequest(df.id, Right(0.2), Array("blade_pressure", "plating_tank")))
    assert(count(dropNA(df2, Seq("blade_pressure", "plating_tank"))) === count(df))

    val df3 = waitDf("df/fillna", FillNARequest(df.id, Left("N/A"), df.schema.fieldNames.toArray))
    assert(count(dropNA(df3, df3.schema.fieldNames)) === 357)

    val df4 = waitDf("df/fillna", FillNARequest(df.id, Left("N/A"), Array("paper_mill_location")))
    assert(count(dropNA(df4, Seq("paper_mill_location"))) === count(df))
  }

  test("fillNAWithMap") {
    val df = getCylinderBands

    def dropNA(df: RemoteDataframe, cols: Seq[String]): RemoteDataframe = {
      waitDf("df/dropna", DropNARequest(df.id, cols.length, cols.toArray))
    }

    val df1 = waitDf("df/fillnawithmap",
      FillNAWithMapRequest(df.id, Map("blade_pressure" -> 2.0, "paper_mill_location" -> "N/A")))
    assert(count(dropNA(df1, Seq("blade_pressure", "paper_mill_location"))) === count(df))

    val ex = intercept[ServerException] {
      waitDf("df/fillnawithmap",
        FillNAWithMapRequest(df.id, Map("blade_pressure" -> 2.0, "non_existed_column" -> "N/A")))
    }
    assert(ex.getMessage.startsWith("Spark query analysis exception: " +
      "Cannot resolve column name \"non_existed_column\""))
  }
  test("replace") {
    val df = getCylinderBands

    def countWhere(df: RemoteDataframe, colName: String, value: Any): Long = {
      count(waitDf("df/where", ColumnsRequest(df.id,
        Array(new Column(EqualTo(df.col(colName).expr, functions.lit(value).expr))))))
    }

    // number of rows with customer == GUIDEPOSTS
    val nRowCustomer = countWhere(df, "customer", "GUIDEPOSTS")
    assert(nRowCustomer > 0)

    val df1 = waitDf("df/replace",
      ReplaceRequest(df.id, Array("customer"), Map("GUIDEPOSTS" -> "GUIDEPOSTS-replaced")))
    assert(countWhere(df1, "customer", "GUIDEPOSTS") === 0)
    assert(countWhere(df1, "customer", "GUIDEPOSTS-replaced") === nRowCustomer)

    val nRowJobNumber = countWhere(df, "job_number", 47103)
    assert(nRowJobNumber > 0)
    val df2 = waitDf("df/replace", ReplaceRequest(df.id, Array("unit_number", "job_number"), Map(47103 -> 42)))
    assert(countWhere(df2, "unit_number", 47103) === 0)
    assert(countWhere(df2, "job_number", 47103) === 0)
    assert(countWhere(df2, "job_number", 42) === nRowJobNumber)

    // elements in valueMaps must have the same data type
    val ex = intercept[ServerException] {
      waitDf("df/replace", ReplaceRequest(df.id, Array("unit_number", "job_number"),
        Map(47103 -> 42, "GUIDEPOSTS" -> "GUIDEPOSTS-replaced")))
    }
    assert(ex.getMessage.startsWith("requirement failed: Only support replacement " +
      "map of type String, Double or Boolean"))

    // replace a value with null
    val df4 = waitDf("df/replace", ReplaceRequest(df.id, Array("unit_number", "customer"),
      Map("bbbb" -> "aaaa", "GUIDEPOSTS" -> null)))
    assert(countWhere(df4, "customer", "GUIDEPOSTS") === 0)
    assert(count(waitDf("df/where", ColumnsRequest(df4.id,
      Array(new Column(IsNull(df4.col("customer").expr)))))) === nRowCustomer)
  }

  //////////////////////////////////////////////////////
  // SQL commands
  //////////////////////////////////////////////////////

  test("WithColumn") {
    val df = getCylinderBands

    val df1 = waitDf("df/withcolumn",
      WithColumnRequest(df.id, "timestamp2", new Column(Divide(df.col("timestamp").expr, functions.lit(2).expr))))
    assert(df1.schema.length === df.schema.length + 1)
    assert(df1.schema.get("timestamp2").nonEmpty)
    val sample1 = take(df1, 100)
    assert(sample1.get[Long]("timestamp").get.zip(sample1.get[Double]("timestamp2").get).forall {
      case (v1: Long, v2: Double) => v1 / 2.0 === v2
    })
  }

  test("WithColumnRenamed") {
    val df = getCylinderBands

    val df1 = waitDf("df/withcolumnrenamed", WithColumnRenamedRequest(df.id, "timestamp", "timestamp2"))
    assert(df1.schema.length === df.schema.length)
    assert(df.id !== df1.id)
    assert(df1.schema.get("timestamp").isEmpty)
    assert(df1.schema.get("timestamp2").nonEmpty)
    assert(df1.schema("timestamp2").storageType === df.schema("timestamp").storageType)
    assert(df1.schema("timestamp2").variableType === df.schema("timestamp").variableType)
  }

  test("Select") {
    val df = getCylinderBands

    val df1 = waitDf("df/select", ColumnsRequest(df.id,
      Array(df.col("customer"), df.col("timestamp"),
        new Column(
          Alias(CaseWhen(Seq(
            (EqualTo(Remainder(df.col("timestamp").expr, functions.lit(2).expr), functions.lit(0).expr),
              functions.lit("even").expr)),
            Some(functions.lit("odd").expr)),
            "caseWhenTs")
        ))
    ))
    assert(count(df) === count(df1))
    assert(df1.schema.fieldNames === Seq("customer", "timestamp", "caseWhenTs"))
    assert(df1.schema("caseWhenTs").storageType === StorageTypes.StringType)
    val sample1 = take(df1, 100)
    assert(sample1.get[Long]("timestamp").get.zip(sample1.get[String]("caseWhenTs").get).forall {
      case (t: Long, "even") => t % 2 === 0
      case (t: Long, "odd") => t % 2 === 1
    })
  }

  test("where") {
    val df = getCylinderBands

    val df1 = waitDf("df/where", ColumnsRequest(df.id,
      Array(new Column(EqualTo(Remainder(df.col("timestamp").expr, functions.lit(2).expr), functions.lit(0).expr)))
    ))
    assert(count(df1) < count(df))
    assert(df1.schema === df.schema)
    val sample1 = take(df1, 100)
    assert(sample1.get[Long]("timestamp").get.forall(_ % 2 === 0))

    val ex = intercept[ServerException] {
      waitDf("df/where", ColumnsRequest(df.id, Array(df.col("timestamp"), df.col("customer"))))
    }
    assert(ex.getMessage === "requirement failed: 'Where' takes 1 column as its argument, got 2")
  }

  test("Alias") {
    val df = getCylinderBands

    val df1 = waitDf("df/alias", AliasRequest(df.id, "my_alias"))
    assert(df1.id !== df.id)
    assert(df1.schema === df.schema)
    assert(count(df1) === count(df))
  }

  test("Join and Broadcast") {
    val df = getCylinderBands

    val df1NoAlias = waitDf("df/where", ColumnsRequest(df.id,
      Array(new Column(In(df.col("customer").expr,
        Seq(functions.lit("GUIDEPOSTS").expr, functions.lit("ECKERD").expr))))))
    val df1 = waitDf("df/alias", AliasRequest(df1NoAlias.id, "small"))
    val df2NoAlias = waitDf("df/where", ColumnsRequest(df.id,
      Array(new Column(In(df.col("customer").expr,
        Seq(functions.lit("GUIDEPOSTS").expr, functions.lit("ECKERD").expr, functions.lit("TARGET").expr))))))
    val df2 = waitDf("df/alias", AliasRequest(df2NoAlias.id, "big"))

    // inner join
    val dfj1 = waitDf("df/join", JoinRequest(df1.id, df2.id,
      functions.col("small.customer") === functions.col("big.customer"), "inner"))
    assert(count(dfj1) === 117)

    // outer join
    val dfj2 = waitDf("df/join", JoinRequest(df1.id, df2.id,
      functions.col("small.customer") === functions.col("big.customer"), "outer"))
    assert(count(dfj2) === 157)

    val ex = intercept[ServerException] {
      waitDf("df/join", JoinRequest(df1.id, df2.id,
        functions.col("small.customer") === functions.col("big.customer"), "outer_unrecognized"))
    }
    assert(ex.getMessage.startsWith("Unsupported join type 'outer_unrecognized'"))

    // join with broadcast
    val df1b = waitDf("df/broadcast", DataframeRequest(df1.id))
    val dfj3 = waitDf("df/join", JoinRequest(df1b.id, df2.id,
      functions.col("small.customer") === functions.col("big.customer"), "inner"))
    assert(count(dfj3) === 117)
  }

  test("Limit") {
    val df = getCylinderBands

    val df1 = waitDf("df/limit", LimitRequest(df.id, 100))
    assert(df1.id !== df.id)
    assert(df1.schema === df.schema)
    assert(count(df1) === 100)

    val ex = intercept[ServerException] {
      waitDf("df/limit", LimitRequest(df.id, -100))
    }
    assert(ex.getMessage === "requirement failed: The limit must be equal to or greater than 0, but got -100")
  }

  test("Union") {
    val df = waitDf("df/limit", LimitRequest(getCylinderBands.id, 20))

    val df2 = waitDf("df/union", DataframeSetRequest(df.id, df.id))
    assert(count(df2) === 2 * count(df))
    assert(df2.schema === df.schema)

    val df3 = waitDf("df/dropcolumns", ColumnNamesRequest(df.id, Array("customer", "job_number")))
    assert(df3.schema.size === df.schema.size - 2)

    val ex = intercept[ServerException] {
      waitDf("df/union", DataframeSetRequest(df.id, df3.id))
    }
    assert(ex.getMessage.startsWith("requirement failed: Unions only work for tables " +
      "with the same number of columns, but got"))
  }

  test("Intersect") {
    val df1 = sendSql(s"SELECT * FROM $cylinderBandsTableName WHERE customer LIKE 'BELK'")
    val df2 = sendSql(s"SELECT * FROM $cylinderBandsTableName WHERE " +
      s"customer LIKE 'BELK' OR customer LIKE 'AMES'")
    assert(count(df1) === 4)
    val df3 = waitDf("df/intersect", DataframeSetRequest(df1.id, df2.id))
    assert(count(df3) === 4)

    // self intersect
    val df4 = sendSql(s"SELECT * FROM $cylinderBandsTableName LIMIT 30")
    val df5 = waitDf("df/intersect", DataframeSetRequest(df4.id, df4.id))
    assert(count(df5) === count(df4))

    val df6 = waitDf("df/dropcolumns", ColumnNamesRequest(df4.id, Array("customer", "job_number")))
    val ex = intercept[ServerException] {
      waitDf("df/intersect", DataframeSetRequest(df4.id, df6.id))
    }
    assert(ex.getMessage.startsWith("requirement failed: Intersects only work for tables " +
      "with the same number of columns, but got"))
  }

  test("except") {

    val df1 = sendSql(s"SELECT * FROM $cylinderBandsTableName WHERE customer LIKE 'BELK'")
    val df2 = sendSql(s"SELECT * FROM $cylinderBandsTableName WHERE " +
      s"customer LIKE 'BELK' OR customer LIKE 'AMES'")
    assert(count(df1) === 4)
    val df3 = waitDf("df/except", DataframeSetRequest(df2.id, df1.id))
    assert(count(df3) === count(df2) - count(df1))

    // self except
    val df4 = sendSql(s"SELECT * FROM $cylinderBandsTableName LIMIT 30")
    val df5 = waitDf("df/except", DataframeSetRequest(df4.id, df4.id))
    assert(count(df5) === 0)

    val df6 = waitDf("df/dropcolumns", ColumnNamesRequest(df4.id, Array("customer", "job_number")))
    val ex = intercept[ServerException] {
      waitDf("df/except", DataframeSetRequest(df4.id, df6.id))
    }
    assert(ex.getMessage.startsWith("requirement failed: Excepts only work for " +
      "tables with the same number of columns, but got"))
  }
}