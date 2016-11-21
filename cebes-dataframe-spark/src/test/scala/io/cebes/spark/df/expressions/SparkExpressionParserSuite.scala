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
 * Created by phvu on 15/11/2016.
 */

package io.cebes.spark.df.expressions

import io.cebes.df.expressions.Expression
import io.cebes.spark.helpers.TestDataHelper
import org.apache.spark.sql.{Column => SparkColumn}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class SparkExpressionParserSuite extends FunSuite with BeforeAndAfterAll with TestDataHelper {

  override def beforeAll(): Unit = {
    super.beforeAll()
    createOrReplaceCylinderBands()
  }

  test("parser with simple spark primitive column") {
    val parser = new SparkExpressionParser()

    val sparkCol = new SparkColumn("abc")
    parser.parse(SparkPrimitiveExpression(sparkCol))
    assert(parser.getResult.isInstanceOf[SparkColumn])

    val exp = intercept[RuntimeException] {
      parser.parse(new Expression {
        override def children: Seq[Expression] = Seq.empty
      })
    }
    assert(exp.getMessage.contains("Visit method not found"))
  }

  test("parser with DF col()") {
    val df = sparkDataframeService.sql(s"SELECT * FROM $cylinderBandsTableName")
    val sparkCol = SparkExpressionParser.toSparkColumn(df.col("timestamp"))
    assert(sparkCol.isInstanceOf[SparkColumn])

    val sparkCols = SparkExpressionParser.toSparkColumns(df.col("Timestamp"), df.col("cylinder_number"))
    assert(sparkCols.length === 2)
  }
}
