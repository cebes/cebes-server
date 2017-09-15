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
 * Created by phvu on 24/12/2016.
 */

package io.cebes.server.routes.df

import java.util.UUID

import io.cebes.df.DataframeService.AggregationTypes
import io.cebes.df.expressions.{Expression, Literal, Pow}
import io.cebes.df.{Column, functions}
import io.cebes.server.routes.df.HttpDfJsonProtocol._
import io.cebes.spark.df.expressions.SparkPrimitiveExpression
import io.cebes.spark.json.CebesSparkJsonProtocol._
import org.scalatest.FunSuite
import spray.json._

class HttpDfJsonProtocolSuite extends FunSuite {

  test("SparkPrimitiveExpression") {
    val expr: Expression = SparkPrimitiveExpression(UUID.randomUUID(), "timestamp", None)
    val exprStr = expr.toJson.compactPrint
    val expr2 = exprStr.parseJson.convertTo[Expression]
    assert(expr2.isInstanceOf[SparkPrimitiveExpression])
    assert(expr2 === expr)
  }

  test("Column") {
    val col = new Column(Pow(SparkPrimitiveExpression(UUID.randomUUID(), "col_blah", None), Literal(200)))
    val colStr = col.toJson.compactPrint
    val col2 = colStr.parseJson.convertTo[Column]
    assert(col2.expr === col.expr)
  }

  test("FillNAWithMapRequest") {
    val r = FillNAWithMapRequest(UUID.randomUUID(),
      Map("a" -> 10, "b" -> 100L, "c" -> 20.0f, "d" -> 100.0, "e" -> "aaaa", "f" -> true, "g" -> false))
    val s = r.toJson.compactPrint

    val r2 = s.parseJson.convertTo[FillNAWithMapRequest]
    assert(r === r2)
  }

  test("ReplaceRequest") {
    val r1 = ReplaceRequest(UUID.randomUUID(), Array("a", "b", "c"),
      Map("x" -> "y", "aaa" -> "", "bb" -> null))
    val s1 = r1.toJson.compactPrint
    val r2 = s1.parseJson.convertTo[ReplaceRequest]
    assert(r2.df === r1.df)
    assert(r2.colNames === r1.colNames)
    assert(r2.replacement("x") === "y")
    assert(r2.replacement("aaa") === "")
    assert(r2.replacement("bb") === null)
    assert(r2.replacement.size === 3)

    val r3 = ReplaceRequest(UUID.randomUUID(), Array("a", "b", "c"),
      Map(10.0 -> 0.9, -10.0 -> 20.5))
    val s3 = r3.toJson.compactPrint
    val r4 = s3.parseJson.convertTo[ReplaceRequest]
    assert(r4.df === r3.df)
    assert(r4.colNames === r3.colNames)
    assert(r4.replacement(10.0) === 0.9)
    assert(r4.replacement(-10.0) === 20.5)

    val r5 = ReplaceRequest(UUID.randomUUID(), Array("a", "b", "c"),
      Map(true -> false, false -> true))
    val s5 = r5.toJson.compactPrint
    val r6 = s5.parseJson.convertTo[ReplaceRequest]
    assert(r6.df === r5.df)
    assert(r6.colNames === r5.colNames)
    assert(r6.replacement(true) === false)
    assert(r6.replacement(false) === true)
  }

  test("SampleByRequest") {
    val r1 = SampleByRequest(UUID.randomUUID(), "abc", Map("aaaa" -> 0.1, "ccc" -> 0.2), 42)
    val s1 = r1.toJson.compactPrint
    val r2 = s1.parseJson.convertTo[SampleByRequest]
    assert(r1 === r2)

    val r3 = SampleByRequest(UUID.randomUUID(), "abcd", Map(true -> 0.1, false -> 0.3), 42)
    val s3 = r3.toJson.compactPrint
    val r4 = s3.parseJson.convertTo[SampleByRequest]
    assert(r3 === r4)

    val r5 = SampleByRequest(UUID.randomUUID(), "abcd", Map(1000 -> 0.1, 20000 -> 0.9), 42)
    val s5 = r5.toJson.compactPrint
    val r6 = s5.parseJson.convertTo[SampleByRequest]
    assert(r5 === r6)
  }

  test("AggregateRequest") {
    val r1 = AggregateRequest(UUID.randomUUID(), Array(functions.col("customer")), AggregationTypes.RollUp,
      None, None, Some(Array(functions.lit(100.0f))), None, Array())
    val s1 = r1.toJson.compactPrint
    val r2 = s1.parseJson.convertTo[AggregateRequest]
    assert(r2.df === r1.df)
    assert(r2.aggType === r1.aggType)
    assert(r2.pivotColName === r1.pivotColName)
    assert(r2.pivotValues === r1.pivotValues)
    assert(r2.aggFunc === r1.aggFunc)
    assert(r2.aggColNames === r1.aggColNames)
    assert(r2.cols.length === 1)
    assert(r2.genericAggExprs.nonEmpty)
    assert(r2.genericAggExprs.get.length === 1)

    val r3 = AggregateRequest(UUID.randomUUID(), Array(), AggregationTypes.RollUp,
      Some("col1"), Some(Array(100, 200, 300)), None, Some("count"), Array())
    val s3 = r3.toJson.compactPrint
    val r4 = s3.parseJson.convertTo[AggregateRequest]
    assert(r4.df === r3.df)
    assert(r4.aggType === r3.aggType)
    assert(r4.pivotColName === r3.pivotColName)
    assert(r4.pivotValues.nonEmpty)
    assert(r4.pivotValues.get === Array(100, 200, 300))
    assert(r4.aggFunc === r3.aggFunc)
    assert(r4.aggColNames === r3.aggColNames)
    assert(r4.cols.length === 0)
    assert(r4.genericAggExprs.isEmpty)

    val r5 = AggregateRequest(UUID.randomUUID(), Array(), AggregationTypes.RollUp,
      Some("col1"), Some(Array("a", "vvv", "ttt")), None, Some("min"), Array("col3", "col4"))
    val s5 = r5.toJson.compactPrint
    val r6 = s5.parseJson.convertTo[AggregateRequest]
    assert(r6.df === r5.df)
    assert(r6.aggType === r5.aggType)
    assert(r6.pivotColName === r5.pivotColName)
    assert(r6.pivotValues.nonEmpty)
    assert(r6.pivotValues.get === Array("a", "vvv", "ttt"))
    assert(r6.aggFunc === r5.aggFunc)
    assert(r6.aggColNames === r5.aggColNames)
    assert(r6.cols.length === 0)
    assert(r6.genericAggExprs.isEmpty)

    val r7 = AggregateRequest(UUID.randomUUID(), Array(), AggregationTypes.RollUp,
      Some("col1"), None, None, Some("count"), Array())
    val s7 = r7.toJson.compactPrint
    val r8 = s7.parseJson.convertTo[AggregateRequest]
    assert(r8.df === r7.df)
    assert(r8.aggType === r7.aggType)
    assert(r8.pivotColName === r7.pivotColName)
    assert(r8.pivotValues === r7.pivotValues)
    assert(r8.aggFunc === r7.aggFunc)
    assert(r8.aggColNames === r7.aggColNames)
    assert(r8.cols.length === 0)
    assert(r8.genericAggExprs.isEmpty)
  }
}
