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
import io.cebes.df.DataframeService.AggregationTypes
import io.cebes.df.expressions._
import io.cebes.df.sample.DataSample
import io.cebes.df.types.{StorageTypes, VariableTypes}
import io.cebes.df.{Column, functions}
import io.cebes.http.client.ServerException
import io.cebes.json.CebesCoreJsonProtocol._
import io.cebes.server.client.RemoteDataframe
import io.cebes.server.routes.AbstractRouteSuite
import io.cebes.server.routes.common.HttpServerJsonProtocol._
import io.cebes.server.routes.common.HttpTagJsonProtocol._
import io.cebes.server.routes.common.{TagAddRequest, TagDeleteRequest, TaggedDataframeResponse, TagsGetRequest}
import io.cebes.server.routes.df.HttpDfJsonProtocol._
import io.cebes.tag.Tag
import spray.json.DefaultJsonProtocol.{DoubleJsonFormat, LongJsonFormat, arrayFormat}

class DataframeHandlerSuite extends AbstractRouteSuite {

  /**
    * Private helper for taking a sample of the given dataframe
    */
  private def take(df: RemoteDataframe, n: Int): DataSample = {
    val sample = request[LimitRequest, DataSample]("df/take", LimitRequest(df.id, n))
    assert(sample.schema === df.schema)
    assert(sample.data.length === sample.schema.size)
    assert(sample.data.forall(_.lengthCompare(n) <= 0))
    sample
  }

  /////////////////////////////////////////////
  // tests
  /////////////////////////////////////////////

  test("tagAdd, tagDelete and get") {
    val df = requestDf("df/limit", LimitRequest(getCylinderBands.id, 100))
    assert(count(df) === 100)

    try {
      requestDf("df/tagdelete", TagDeleteRequest(Tag.fromString("tag1:default")))
    } catch {
      case _: ServerException =>
    }

    // random UUID
    val ex0 = intercept[ServerException] {
      requestDf("df/tagadd", TagAddRequest(Tag.fromString("tag1"), UUID.randomUUID()))
    }
    assert(ex0.getMessage.startsWith("Object ID not found:"))

    // valid request
    requestDf("df/tagadd", TagAddRequest(Tag.fromString("tag1"), df.id))

    // duplicated tag
    val ex1 = intercept[ServerException] {
      requestDf("df/tagadd", TagAddRequest(Tag.fromString("tag1"), getCylinderBands.id))
    }
    assert(ex1.getMessage.startsWith("Tag tag1:default already exists"))

    val ex2 = intercept[ServerException] {
      requestDf("df/tagadd", TagAddRequest(Tag.fromString("tag1:default"), getCylinderBands.id))
    }
    assert(ex2.getMessage.startsWith("Tag tag1:default already exists"))

    // get tag
    val df2 = requestDf("df/get", "tag1:default")
    assert(df2.id === df.id)
    assert(count(df2) === 100)

    // delete tag
    requestDf("df/tagdelete", TagDeleteRequest(Tag.fromString("tag1:default")))

    // cannot get the tag again
    val ex3 = intercept[ServerException](requestDf("df/get", "tag1:default"))
    assert(ex3.getMessage.startsWith("Tag not found: tag1:default"))

    // cannot delete non-existed tag
    val ex4 = intercept[ServerException](requestDf("df/tagdelete", TagDeleteRequest(Tag.fromString("tag1:default"))))
    assert(ex4.getMessage.startsWith("Tag not found: tag1:default"))

    // but can get the Dataframe using its ID
    val df3 = requestDf("df/get", df.id.toString)
    assert(!df3.eq(df))
    assert(df3.id === df.id)
    assert(count(df3) === 100)
  }

  test("df/get fail cases") {
    // invalid UUID
    val ex1 = intercept[ServerException](requestDf("df/get", UUID.randomUUID().toString))
    assert(ex1.getMessage.startsWith("ID not found"))

    // valid but non-existent tag
    val ex2 = intercept[ServerException](requestDf("df/get", "surreal-tag:surreal-version911"))
    assert(ex2.getMessage.startsWith("Tag not found"))

    // invalid tag
    val ex3 = intercept[ServerException](requestDf("df/get", "This is invalid tag/abc:version 1"))
    assert(ex3.getMessage.startsWith("Failed to parse Id or Tag"))
  }

  test("tags complicated case") {
    val df1 = requestDf("df/limit", LimitRequest(getCylinderBands.id, 100))
    assert(count(df1) === 100)
    val df2 = requestDf("df/limit", LimitRequest(getCylinderBands.id, 200))
    assert(count(df2) === 200)

    val tag1a = Tag.fromString("cebes.io/df1:v1")
    val tag1b = Tag.fromString("cebes.io/df1:v2")
    val tag2a = Tag.fromString("google.com/df2")

    (tag1a :: tag1b :: tag2a :: Nil).foreach { t =>
      try {
        requestDf("df/tagdelete", TagDeleteRequest(t))
      } catch {
        case _: ServerException =>
      }
    }

    val df1a = requestDf("df/tagadd", TagAddRequest(tag1a, df1.id))
    assert(df1a.id === df1.id)
    val df1b = requestDf("df/tagadd", TagAddRequest(tag1b, df1.id))
    assert(df1b.id === df1.id)
    val df2a = requestDf("df/tagadd", TagAddRequest(tag2a, df2.id))
    assert(df2a.id === df2.id)

    // no filter
    val tags = request[TagsGetRequest, Array[TaggedDataframeResponse]]("df/tags", TagsGetRequest(None))
    assert(tags.length >= 3)
    assert(tags.exists(_.tag === tag1a))
    assert(tags.exists(_.tag === tag1b))
    assert(tags.exists(_.tag === tag2a))

    // with filter
    val tags2 = request[TagsGetRequest, Array[TaggedDataframeResponse]]("df/tags",
      TagsGetRequest(Some("cebes*/df?:v2")))
    assert(tags2.nonEmpty)
    assert(tags2.exists(_.tag === tag1b))

    val tags3 = request[TagsGetRequest, Array[TaggedDataframeResponse]]("df/tags",
      TagsGetRequest(Some("google.com/df?:*")))
    assert(tags3.nonEmpty)
    assert(tags3.exists(_.tag === tag2a))

    // delete tags
    requestDf("df/tagdelete", TagDeleteRequest(tag1a))
    requestDf("df/tagdelete", TagDeleteRequest(tag1b))
    requestDf("df/tagdelete", TagDeleteRequest(tag2a))

    val tags4 = request[TagsGetRequest, Array[TaggedDataframeResponse]]("df/tags",
      TagsGetRequest(Some("google.com/df2:*")))
    assert(tags4.isEmpty)
  }

  test("count") {
    val df = getCylinderBands

    val rows = request[DataframeRequest, Long]("df/count", DataframeRequest(df.id))
    assert(rows === 540)

    val ex = intercept[ServerException] {
      request[DataframeRequest, Long]("df/count", DataframeRequest(UUID.randomUUID()))
    }
    assert(ex.message.startsWith("Object ID not found"))
  }

  test("InferVariableTypes") {
    val df = getCylinderBands
    assert(df.schema("timestamp").variableType === VariableTypes.DISCRETE)

    val df1 = requestDf("df/infervariabletypes", LimitRequest(df.id, 10000))
    assert(df1.id !== df.id)
    assert(count(df1) === count(df))
    assert(df1.schema.fieldNames === df.schema.fieldNames)
    assert(df1.schema("timestamp").variableType === VariableTypes.ORDINAL)
  }

  test("WithVariableTypes") {
    val df = getCylinderBands
    assert(df.schema("timestamp").variableType === VariableTypes.DISCRETE)
    assert(df.schema("customer").variableType === VariableTypes.TEXT)

    val df1 = requestDf("df/withvariabletypes", WithVariableTypesRequest(df.id,
      Map("timestamp" -> VariableTypes.ORDINAL,
        "customer" -> VariableTypes.NOMINAL)))
    assert(df1.id !== df.id)
    assert(count(df1) === count(df))
    assert(df1.schema.fieldNames === df.schema.fieldNames)
    assert(df1.schema("timestamp").variableType === VariableTypes.ORDINAL)
    assert(df1.schema("customer").variableType === VariableTypes.NOMINAL)
  }

  test("WithStorageTypes") {
    val df = getCylinderBands
    assert(df.schema("job_number").storageType === StorageTypes.IntegerType)

    val df1 = requestDf("df/withstoragetypes", WithStorageTypesRequest(df.id,
      Map("job_number" -> StorageTypes.LongType)))
    assert(df1.id !== df.id)
    assert(count(df1) === count(df))
    assert(df1.schema.fieldNames === df.schema.fieldNames)
    assert(df1.schema("job_number").storageType === StorageTypes.LongType)
  }

  test("take") {
    val df = getCylinderBands

    val sample = request[LimitRequest, DataSample]("df/take", LimitRequest(df.id, 15))
    assert(sample.schema === df.schema)
    assert(sample.data.length === sample.schema.size)
    assert(sample.data.forall(_.length === 15))

    val ex1 = intercept[ServerException] {
      request[LimitRequest, DataSample]("df/take", LimitRequest(df.id, -15))
    }
    assert(ex1.getMessage.startsWith("The limit expression must be equal to or greater than 0"))

    val ex2 = intercept[ServerException] {
      request[LimitRequest, DataSample]("df/take", LimitRequest(UUID.randomUUID(), 15))
    }
    assert(ex2.message.startsWith("Object ID not found"))
  }

  test("sample") {
    val df = getCylinderBands

    val df2 = requestDf("df/sample", SampleRequest(df.id, withReplacement = true, 0.5, 42))
    assert(df2.schema === df.schema)
    assert(df2.id !== df.id)

    val ex = intercept[ServerException] {
      requestDf("df/sample", SampleRequest(UUID.randomUUID(), withReplacement = true, 0.5, 42))
    }
    assert(ex.message.startsWith("Object ID not found"))
  }

  test("sort") {
    val df = getCylinderBands

    val df1 = requestDf("df/sort", ColumnsRequest(df.id, Array(df.col("timestamp").desc, df.col("customer").asc)))
    assert(df1.schema === df.schema)

    val sample1 = take(df1, 50)
    val colTimestamp = sample1.get[Long]("timestamp").get
    assert(colTimestamp.zip(colTimestamp.tail).forall(t => t._1 >= t._2))

    val ex = intercept[ServerException] {
      requestDf("df/sort", ColumnsRequest(df.id, Array(df.col("non_existed_column").asc)))
    }
    assert(ex.getMessage.startsWith("Spark query analysis exception: Cannot resolve column name"))
  }

  test("drop") {
    val df = getCylinderBands

    val df1 = requestDf("df/dropcolumns", ColumnNamesRequest(df.id, Array("TimeStamp", "ink_cOlor", "wrong_column")))
    assert(df1.schema.size === df.schema.size - 2)
    assert(df1.schema.get("timestamp").isEmpty)
    assert(df1.schema.get("ink_cOlor").isEmpty)

    val df2 = requestDf("df/dropcolumns", ColumnNamesRequest(df.id, Array()))
    assert(df2.schema.size === df.schema.size)
    assert(df2.schema.get("timestamp").nonEmpty)
    assert(df2.schema.get("ink_cOlor").nonEmpty)
    assert(df2.schema === df.schema)
  }

  test("dropDuplicates") {
    val df = getCylinderBands

    val df1 = requestDf("df/dropduplicates", ColumnNamesRequest(df.id, Array("TimeStamp", "ink_cOlor")))
    assert(df1.schema === df.schema)

    val ex1 = intercept[ServerException] {
      requestDf("df/dropduplicates", ColumnNamesRequest(df.id, Array("TimeStamp", "ink_cOlor", "wrong_column")))
    }
    assert(ex1.getMessage.startsWith("Spark query analysis exception: Cannot resolve column name \"wrong_column\""))

    val df2 = requestDf("df/dropcolumns", ColumnNamesRequest(df.id, Array()))
    assert(df2.schema === df.schema)
  }

  ////////////////////
  // NA functions
  ////////////////////

  test("DropNA") {
    val df = getCylinderBands
    assert(count(df) === 540)

    // any, on all columns
    val df1 = requestDf("df/dropna", DropNARequest(df.id, df.schema.fields.length, df.schema.fieldNames.toArray))
    assert(df1.schema === df.schema)
    assert(count(df1) === 357)

    // all, on all columns
    val df2 = requestDf("df/dropna", DropNARequest(df.id, 1, df.schema.fieldNames.toArray))
    assert(count(df2) === count(df))

    // any, on some columns
    val df3 = requestDf("df/dropna", DropNARequest(df.id, 2, Array("plating_tank", "grain_screened")))
    assert(count(df3) === 522)

    intercept[ServerException](requestDf("df/dropna", DropNARequest(df.id, 1, Array("non_existed_column"))))

    val df6 = requestDf("df/dropna", DropNARequest(df.id, 1, Array("plating_tank", "proof_cut")))
    assert(count(df6) === 535)

    // minNonNull on some columns
    val df8 = requestDf("df/dropna", DropNARequest(df.id, 2, Array("grain_screened", "plating_tank", "proof_cut")))
    assert(count(df8) === 535)
  }

  test("fillNA") {
    val df = getCylinderBands

    def dropNA(df: RemoteDataframe, cols: Seq[String]): RemoteDataframe = {
      requestDf("df/dropna", DropNARequest(df.id, cols.length, cols.toArray))
    }

    val df1 = requestDf("df/fillna", FillNARequest(df.id, Right(0.2), df.schema.fieldNames.toArray))
    assert(count(dropNA(df1, df1.schema.fieldNames)) === count(df))

    val df2 = requestDf("df/fillna", FillNARequest(df.id, Right(0.2), Array("blade_pressure", "plating_tank")))
    assert(count(dropNA(df2, Seq("blade_pressure", "plating_tank"))) === count(df))

    val df3 = requestDf("df/fillna", FillNARequest(df.id, Left("N/A"), df.schema.fieldNames.toArray))
    assert(count(dropNA(df3, df3.schema.fieldNames)) === 357)

    val df4 = requestDf("df/fillna", FillNARequest(df.id, Left("N/A"), Array("paper_mill_location")))
    assert(count(dropNA(df4, Seq("paper_mill_location"))) === count(df))
  }

  test("fillNAWithMap") {
    val df = getCylinderBands

    def dropNA(df: RemoteDataframe, cols: Seq[String]): RemoteDataframe = {
      requestDf("df/dropna", DropNARequest(df.id, cols.length, cols.toArray))
    }

    val df1 = requestDf("df/fillnawithmap",
      FillNAWithMapRequest(df.id, Map("blade_pressure" -> 2.0, "paper_mill_location" -> "N/A")))
    assert(count(dropNA(df1, Seq("blade_pressure", "paper_mill_location"))) === count(df))

    val ex = intercept[ServerException] {
      requestDf("df/fillnawithmap",
        FillNAWithMapRequest(df.id, Map("blade_pressure" -> 2.0, "non_existed_column" -> "N/A")))
    }
    assert(ex.getMessage.startsWith("Spark query analysis exception: " +
      "Cannot resolve column name \"non_existed_column\""))
  }
  test("replace") {
    val df = getCylinderBands

    def countWhere(df: RemoteDataframe, colName: String, value: Any): Long = {
      count(requestDf("df/where", ColumnsRequest(df.id,
        Array(new Column(EqualTo(df.col(colName).expr, functions.lit(value).expr))))))
    }

    // number of rows with customer == GUIDEPOSTS
    val nRowCustomer = countWhere(df, "customer", "GUIDEPOSTS")
    assert(nRowCustomer > 0)

    val df1 = requestDf("df/replace",
      ReplaceRequest(df.id, Array("customer"), Map("GUIDEPOSTS" -> "GUIDEPOSTS-replaced")))
    assert(countWhere(df1, "customer", "GUIDEPOSTS") === 0)
    assert(countWhere(df1, "customer", "GUIDEPOSTS-replaced") === nRowCustomer)

    val nRowJobNumber = countWhere(df, "job_number", 47103)
    assert(nRowJobNumber > 0)
    val df2 = requestDf("df/replace", ReplaceRequest(df.id, Array("unit_number", "job_number"), Map(47103 -> 42)))
    assert(countWhere(df2, "unit_number", 47103) === 0)
    assert(countWhere(df2, "job_number", 47103) === 0)
    assert(countWhere(df2, "job_number", 42) === nRowJobNumber)

    // elements in valueMaps must have the same data type
    val ex = intercept[ServerException] {
      requestDf("df/replace", ReplaceRequest(df.id, Array("unit_number", "job_number"),
        Map(47103 -> 42, "GUIDEPOSTS" -> "GUIDEPOSTS-replaced")))
    }
    assert(ex.getMessage.startsWith("requirement failed: Only support replacement " +
      "map of type String, Double or Boolean"))

    // replace a value with null
    val df4 = requestDf("df/replace", ReplaceRequest(df.id, Array("unit_number", "customer"),
      Map("bbbb" -> "aaaa", "GUIDEPOSTS" -> null)))
    assert(countWhere(df4, "customer", "GUIDEPOSTS") === 0)
    assert(count(requestDf("df/where", ColumnsRequest(df4.id,
      Array(new Column(IsNull(df4.col("customer").expr)))))) === nRowCustomer)
  }

  test("approxQuantile") {
    val df = getCylinderBands

    val quantiles = request[ApproxQuantileRequest, Array[Double]]("df/approxquantile",
      ApproxQuantileRequest(df.id, "press", Array(0, 0.1, 0.5, 0.9, 1), 0.01))
    assert(quantiles.length === 5)
    assert(quantiles.forall(d => !d.isInfinity && !d.isNaN))

    // columns with null is fine
    val quantiles2 = request[ApproxQuantileRequest, Array[Double]]("df/approxquantile",
      ApproxQuantileRequest(df.id, "caliper", Array(0, 0.1, 0.5, 0.9, 1), 0.01))
    assert(quantiles2.length === 5)
    assert(quantiles2.forall(d => !d.isInfinity && !d.isNaN))

    // string columns
    val ex2 = intercept[ServerException] {
      request[ApproxQuantileRequest, Array[Double]]("df/approxquantile",
        ApproxQuantileRequest(df.id, "customer", Array(0, 0.1, 0.5, 0.9, 1), 0.01))
    }
    assert(ex2.getMessage === "requirement failed: Quantile calculation for column customer " +
      "with data type StringType is not supported.")
  }

  test("cov") {
    val df = getCylinderBands

    val cov = request[ColumnNamesRequest, Double]("df/cov",
      ColumnNamesRequest(df.id, Array("caliper", "roughness")))
    assert(cov > 0)

    // string column
    val ex = intercept[ServerException] {
      request[ColumnNamesRequest, Double]("df/cov",
        ColumnNamesRequest(df.id, Array("customer", "roughness")))
    }
    assert(ex.getMessage.startsWith("requirement failed: Currently covariance calculation for " +
      "columns with dataType StringType not supported."))

    // more columns than needed
    val ex1 = intercept[ServerException] {
      request[ColumnNamesRequest, Double]("df/cov",
        ColumnNamesRequest(df.id, Array("customer", "roughness", "timestamp")))
    }
    assert(ex1.getMessage.startsWith("requirement failed: Cov requires 2 column names, got 3 columns"))
  }

  test("corr") {
    val df = getCylinderBands

    val corr = request[ColumnNamesRequest, Double]("df/corr",
      ColumnNamesRequest(df.id, Array("caliper", "roughness")))
    assert(corr > 0)

    // string column
    val ex = intercept[ServerException] {
      request[ColumnNamesRequest, Double]("df/corr",
        ColumnNamesRequest(df.id, Array("customer", "roughness")))
    }
    assert(ex.getMessage.startsWith("requirement failed: Currently correlation calculation " +
      "for columns with dataType StringType not supported."))

    // more columns than needed
    val ex1 = intercept[ServerException] {
      request[ColumnNamesRequest, Double]("df/corr",
        ColumnNamesRequest(df.id, Array("customer", "roughness", "timestamp")))
    }
    assert(ex1.getMessage.startsWith("requirement failed: Corr requires 2 column names, got 3 columns"))
  }

  test("crosstab") {
    val df = getCylinderBands

    val df1 = requestDf("df/crosstab", ColumnNamesRequest(df.id, Array("customer", "grain_screened")))
    assert(df1.schema.fieldNames === Seq("customer_grain_screened", "", "NO", "YES"))
    assert(count(df1) < count(df))

    val ex1 = intercept[ServerException] {
      requestDf("df/crosstab", ColumnNamesRequest(df.id, Array("customer", "grain_screened", "timestamp")))
    }
    assert(ex1.getMessage.startsWith("requirement failed: Crosstab requires 2 column names, got 3 columns"))
  }

  test("freqItems") {
    val df = getCylinderBands

    val df1 = requestDf("df/freqitems", FreqItemsRequest(df.id, Array("customer", "grain_screened"), 0.1))
    assert(count(df1) === 1)
    assert(df1.schema.fieldNames === Seq("customer_freqItems", "grain_screened_freqItems"))
    assert(df1.schema("customer_freqItems").storageType === StorageTypes.arrayType(StorageTypes.StringType))
    assert(df1.schema("grain_screened_freqItems").storageType === StorageTypes.arrayType(StorageTypes.StringType))
  }

  test("SampleBy") {
    val df = getCylinderBands

    val df1 = requestDf("df/sampleby", SampleByRequest(df.id, "customer",
      Map("GUIDEPOSTS" -> 0.1, "ECKERD" -> 0.5, "TVGUIDE" -> 0.2), 42))
    assert(df1.schema === df.schema)
    val n = count(df1)
    assert(n > 0 && n < count(df))
  }

  //////////////////////////////////////////////////////
  // SQL commands
  //////////////////////////////////////////////////////

  test("WithColumn") {
    val df = getCylinderBands

    val df1 = requestDf("df/withcolumn",
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

    val df1 = requestDf("df/withcolumnrenamed", WithColumnRenamedRequest(df.id, "timestamp", "timestamp2"))
    assert(df1.schema.length === df.schema.length)
    assert(df.id !== df1.id)
    assert(df1.schema.get("timestamp").isEmpty)
    assert(df1.schema.get("timestamp2").nonEmpty)
    assert(df1.schema("timestamp2").storageType === df.schema("timestamp").storageType)
    assert(df1.schema("timestamp2").variableType === df.schema("timestamp").variableType)
  }

  test("Select") {
    val df = getCylinderBands

    val df1 = requestDf("df/select", ColumnsRequest(df.id,
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

    val df1 = requestDf("df/where", ColumnsRequest(df.id,
      Array(new Column(EqualTo(Remainder(df.col("timestamp").expr, functions.lit(2).expr), functions.lit(0).expr)))
    ))
    assert(count(df1) < count(df))
    assert(df1.schema === df.schema)
    val sample1 = take(df1, 100)
    assert(sample1.get[Long]("timestamp").get.forall(_ % 2 === 0))

    val ex = intercept[ServerException] {
      requestDf("df/where", ColumnsRequest(df.id, Array(df.col("timestamp"), df.col("customer"))))
    }
    assert(ex.getMessage === "requirement failed: 'Where' takes 1 column as its argument, got 2")
  }

  test("Alias") {
    val df = getCylinderBands

    val df1 = requestDf("df/alias", AliasRequest(df.id, "my_alias"))
    assert(df1.id !== df.id)
    assert(df1.schema === df.schema)
    assert(count(df1) === count(df))
  }

  test("Join and Broadcast") {
    val df = getCylinderBands

    val df1NoAlias = requestDf("df/where", ColumnsRequest(df.id,
      Array(new Column(In(df.col("customer").expr,
        Seq(functions.lit("GUIDEPOSTS").expr, functions.lit("ECKERD").expr))))))
    val df1 = requestDf("df/alias", AliasRequest(df1NoAlias.id, "small"))
    val df2NoAlias = requestDf("df/where", ColumnsRequest(df.id,
      Array(new Column(In(df.col("customer").expr,
        Seq(functions.lit("GUIDEPOSTS").expr, functions.lit("ECKERD").expr, functions.lit("TARGET").expr))))))
    val df2 = requestDf("df/alias", AliasRequest(df2NoAlias.id, "big"))

    // inner join
    val dfj1 = requestDf("df/join", JoinRequest(df1.id, df2.id,
      functions.col("small.customer") === functions.col("big.customer"), "inner"))
    assert(count(dfj1) === 117)

    // outer join
    val dfj2 = requestDf("df/join", JoinRequest(df1.id, df2.id,
      functions.col("small.customer") === functions.col("big.customer"), "outer"))
    assert(count(dfj2) === 157)

    val ex = intercept[ServerException] {
      requestDf("df/join", JoinRequest(df1.id, df2.id,
        functions.col("small.customer") === functions.col("big.customer"), "outer_unrecognized"))
    }
    assert(ex.getMessage.startsWith("Unsupported join type 'outer_unrecognized'"))

    // join with broadcast
    val df1b = requestDf("df/broadcast", DataframeRequest(df1.id))
    val dfj3 = requestDf("df/join", JoinRequest(df1b.id, df2.id,
      functions.col("small.customer") === functions.col("big.customer"), "inner"))
    assert(count(dfj3) === 117)
  }

  test("Limit") {
    val df = getCylinderBands

    val df1 = requestDf("df/limit", LimitRequest(df.id, 100))
    assert(df1.id !== df.id)
    assert(df1.schema === df.schema)
    assert(count(df1) === 100)

    val ex = intercept[ServerException] {
      requestDf("df/limit", LimitRequest(df.id, -100))
    }
    assert(ex.getMessage === "requirement failed: The limit must be equal to or greater than 0, but got -100")
  }

  test("Union") {
    val df = requestDf("df/limit", LimitRequest(getCylinderBands.id, 20))

    val df2 = requestDf("df/union", DataframeSetRequest(df.id, df.id))
    assert(count(df2) === 2 * count(df))
    assert(df2.schema === df.schema)

    val df3 = requestDf("df/dropcolumns", ColumnNamesRequest(df.id, Array("customer", "job_number")))
    assert(df3.schema.size === df.schema.size - 2)

    val ex = intercept[ServerException] {
      requestDf("df/union", DataframeSetRequest(df.id, df3.id))
    }
    assert(ex.getMessage.startsWith("requirement failed: Unions only work for tables " +
      "with the same number of columns, but got"))
  }

  test("Intersect") {
    val df = getCylinderBands
    val df1 = requestDf("df/where", ColumnsRequest(df.id, Array(new Column(Like(df.col("customer").expr, "BELK")))))
    val df2 = requestDf("df/where", ColumnsRequest(df.id, Array(new Column(Or(
      Like(df.col("customer").expr, "BELK"), Like(df.col("customer").expr, "AMES"))))))

    assert(count(df1) === 4)
    val df3 = requestDf("df/intersect", DataframeSetRequest(df1.id, df2.id))
    assert(count(df3) === 4)

    // self intersect
    val df4 = requestDf("df/limit", LimitRequest(df.id, 30))
    val df5 = requestDf("df/intersect", DataframeSetRequest(df4.id, df4.id))
    assert(count(df5) === count(df4))

    val df6 = requestDf("df/dropcolumns", ColumnNamesRequest(df4.id, Array("customer", "job_number")))
    val ex = intercept[ServerException] {
      requestDf("df/intersect", DataframeSetRequest(df4.id, df6.id))
    }
    assert(ex.getMessage.startsWith("requirement failed: Intersects only work for tables " +
      "with the same number of columns, but got"))
  }

  test("except") {
    val df = getCylinderBands
    val df1 = requestDf("df/where", ColumnsRequest(df.id, Array(new Column(Like(df.col("customer").expr, "BELK")))))
    val df2 = requestDf("df/where", ColumnsRequest(df.id, Array(new Column(Or(
      Like(df.col("customer").expr, "BELK"), Like(df.col("customer").expr, "AMES"))))))

    assert(count(df1) === 4)
    val df3 = requestDf("df/except", DataframeSetRequest(df2.id, df1.id))
    assert(count(df3) === count(df2) - count(df1))

    // self except
    val df4 = requestDf("df/limit", LimitRequest(df.id, 30))
    val df5 = requestDf("df/except", DataframeSetRequest(df4.id, df4.id))
    assert(count(df5) === 0)

    val df6 = requestDf("df/dropcolumns", ColumnNamesRequest(df4.id, Array("customer", "job_number")))
    val ex = intercept[ServerException] {
      requestDf("df/except", DataframeSetRequest(df4.id, df6.id))
    }
    assert(ex.getMessage.startsWith("requirement failed: Excepts only work for " +
      "tables with the same number of columns, but got"))
  }

  test("Aggregate") {
    val df = requestDf("df/limit", LimitRequest(getCylinderBands.id, 100))

    // simple groupby on customer, with max(unit_number)
    val df1 = requestDf("df/aggregate",
      AggregateRequest(df.id,
        Array(df.col("customer")), AggregationTypes.GroupBy,
        None, None,
        Some(Array(functions.max(df.col("unit_number")))),
        None, Array()))
    assert(count(df1) > 0)
    assert(df1.schema.fieldNames === Seq("customer", "max(unit_number)"))

    // groupby on a computed column, with aggFunc on all columns
    val df2 = requestDf("df/aggregate",
      AggregateRequest(df.id,
        Array(functions.length(df.col("customer"))), AggregationTypes.GroupBy,
        None, None,
        None,
        Some("max"), Array()))
    assert(count(df2) > 0)
    assert(df2.schema.length === 25)

    // unrecognized function
    val ex1 = intercept[ServerException] {
      requestDf("df/aggregate",
        AggregateRequest(df.id,
          Array(functions.length(df.col("customer"))), AggregationTypes.GroupBy,
          None, None,
          None,
          Some("unrecognized_function"), Array()))
    }
    assert(ex1.getMessage.startsWith("Unrecognized Aggregation function"))

    // pivot without values, min on job_number
    val df3 = requestDf("df/aggregate",
      AggregateRequest(df.id,
        Array(df.col("customer")), AggregationTypes.GroupBy,
        Some("grain_screened"), None,
        None,
        Some("min"), Array("job_number")))
    assert(df3.schema.length === 4)
    assert(count(df3) > 0)

    // pivot with values, min on job_number
    val df4 = requestDf("df/aggregate",
      AggregateRequest(df.id,
        Array(df.col("customer")), AggregationTypes.GroupBy,
        Some("grain_screened"), Some(Array("YES", "NO")),
        None,
        Some("min"), Array("job_number")))
    assert(df4.schema.length === 3)
    assert(count(df4) > 0)
    assert(df4.schema.fieldNames === Seq("customer", "YES", "NO"))

    // rollup
    val df5 = requestDf("df/aggregate",
      AggregateRequest(df.id,
        Array(df.col("customer"), df.col("grain_screened")), AggregationTypes.RollUp,
        None, None,
        None,
        Some("mean"), Array()))
    assert(count(df5) > 0)
    assert(df5.schema.length === 26)

    // cube
    val df6 = requestDf("df/aggregate",
      AggregateRequest(df.id,
        Array(df.col("customer"), df.col("grain_screened")), AggregationTypes.Cube,
        None, None,
        None,
        Some("mean"), Array()))
    assert(count(df6) > 0)
    assert(df6.schema.length === 26)
  }
}