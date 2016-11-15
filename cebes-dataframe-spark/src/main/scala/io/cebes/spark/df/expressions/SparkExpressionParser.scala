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

import io.cebes.df.expressions.{AbstractExpressionParser, Column => CebesColumn}
import org.apache.spark.sql.{Column => SparkColumn}

import scala.collection.mutable

object SparkExpressionParser {

  /**
    * Transform a cebes Column into a Spark column
    */
  def toSparkColumn(column: CebesColumn): SparkColumn = {
    val parser = new SparkExpressionParser()
    parser.parse(column.expr)
    parser.getResult
  }

  def toSparkColumns(columns: CebesColumn*): Seq[SparkColumn] = columns.map(toSparkColumn)
}


class SparkExpressionParser extends AbstractExpressionParser {

  private val resultStack = mutable.Stack[SparkColumn]()

  def getResult: SparkColumn = {
    if (resultStack.size != 1) {
      throw new IllegalArgumentException("There is an error when parsing the expression, " +
        "or you haven't called parse() yet?")
    }
    resultStack.head
  }

  def visitSparkPrimitiveExpression(expr: SparkPrimitiveExpression): Unit = {
    resultStack.push(expr.sparkCol)
  }
}
