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

import io.cebes.df.Column
import io.cebes.df.expressions._
import io.cebes.spark.df.schema.SparkSchemaUtils
import org.apache.spark.sql.{Column => SparkColumn, functions => sparkFunctions}


object SparkExpressionParser {

  /**
    * Transform a cebes Column into a Spark column
    */
  def toSparkColumn(column: Column): SparkColumn = {
    val parser = new SparkExpressionParser()
    parser.parse(column.expr)
    parser.getResult
  }

  def toSparkColumns(columns: Column*): Seq[SparkColumn] = columns.map(toSparkColumn)
}


class SparkExpressionParser extends StackExpressionParser[SparkColumn] {

  /////////////////////////////////////////////////////////////////////////////
  // visit functions
  /////////////////////////////////////////////////////////////////////////////

  protected def visitSparkPrimitiveExpression(expr: SparkPrimitiveExpression,
                                              parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(expr.sparkCol)
  }

  protected def visitLiteral(expr: Literal, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.lit(expr.value))
  }

  protected def visitUnresolvedColumnName(expr: UnresolvedColumnName,
                                          parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.col(expr.colName))
  }

  protected def visitSortOrder(expr: SortOrder, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(expr.direction match {
      case Ascending => parsedChildren.head.asc
      case Descending => parsedChildren.head.desc
    })
  }

  protected def visitUnaryMinus(expr: UnaryMinus, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(-parsedChildren.head)
  }

  protected def visitNot(expr: Not, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(!parsedChildren.head)
  }

  protected def visitEqualTo(expr: EqualTo, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head === parsedChildren.last)
  }

  protected def visitGreaterThan(expr: GreaterThan, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head > parsedChildren.last)
  }

  protected def visitLessThan(expr: LessThan, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head < parsedChildren.last)
  }

  protected def visitGreaterThanOrEqual(expr: GreaterThanOrEqual,
                                        parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head >= parsedChildren.last)
  }

  protected def visitLessThanOrEqual(expr: LessThanOrEqual,
                                     parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head <= parsedChildren.last)
  }

  protected def visitLessEqualNullSafe(expr: EqualNullSafe,
                                     parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head <=> parsedChildren.last)
  }

  protected def visitCaseWhen(expr: CaseWhen, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    require(parsedChildren.length >= 2, "Expected a list of branches")

    val branch = sparkFunctions.when(parsedChildren.head, parsedChildren(1))
    Some(parsedChildren.grouped(2).toSeq.tail.foldLeft(branch) { (b, conditionClause) =>
      conditionClause match {
        case Seq(condition, value) => b.when(condition, value)
        case Seq(otherwiseVal) => b.otherwise(otherwiseVal)
      }
    })
  }

  protected def visitIsNaN(expr: IsNaN, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.isNaN)
  }

  protected def visitIsNull(expr: IsNull, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.isNull)
  }

  protected def visitIsNotNull(expr: IsNotNull, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.isNotNull)
  }

  protected def visitOr(expr: Or, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head || parsedChildren.last)
  }

  protected def visitAnd(expr: And, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head && parsedChildren.last)
  }

  protected def visitAdd(expr: Add, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head + parsedChildren.last)
  }

  protected def visitSubtract(expr: Subtract, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head - parsedChildren.last)
  }

  protected def visitMultiply(expr: Multiply, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head * parsedChildren.last)
  }

  protected def visitDivide(expr: Divide, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head / parsedChildren.last)
  }

  protected def visitRemainder(expr: Remainder, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head % parsedChildren.last)
  }

  protected def visitIn(expr: In, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.isin(parsedChildren.tail: _*))
  }

  protected def visitLike(expr: Like, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.like(expr.literal))
  }

  protected def visitRLike(expr: RLike, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.rlike(expr.literal))
  }

  protected def visitGetItem(expr: GetItem, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.getItem(parsedChildren.last))
  }

  protected def visitGetField(expr: GetField, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.getField(expr.fieldName))
  }

  protected def visitSubstring(expr: Substring, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.substr(parsedChildren(1), parsedChildren(2)))
  }

  protected def visitContains(expr: Contains, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.contains(parsedChildren.last))
  }

  protected def visitStartsWith(expr: StartsWith, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.startsWith(parsedChildren.last))
  }

  protected def visitEndsWith(expr: EndsWith, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.endsWith(parsedChildren.last))
  }

  protected def visitMultiAlias(expr: MultiAlias, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.as(expr.aliases))
  }

  protected def visitAlias(expr: Alias, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.alias(expr.alias))
  }
  protected def visitCast(expr: Cast, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.cast(SparkSchemaUtils.cebesTypesToSpark(expr.to)))
  }

  protected def visitBitwiseOr(expr: BitwiseOr, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.bitwiseOR(parsedChildren.last))
  }

  protected def visitBitwiseAnd(expr: BitwiseAnd, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.bitwiseAND(parsedChildren.last))
  }

  protected def visitBitwiseXor(expr: BitwiseXor, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.bitwiseXOR(parsedChildren.last))
  }

  protected def visitApproxCountDistinct(expr: ApproxCountDistinct,
                                         parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.approxCountDistinct(parsedChildren.head, expr.relativeSD))
  }

  protected def visitAverage(expr: Average,
                             parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.avg(parsedChildren.head))
  }

  protected def visitCollectList(expr: CollectList,
                                 parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.collect_list(parsedChildren.head))
  }

  protected def visitCollectSet(expr: CollectSet,
                                 parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.collect_set(parsedChildren.head))
  }

  protected def visitCorr(expr: Corr,
                          parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.corr(parsedChildren.head, parsedChildren.last))
  }

  protected def visitCount(expr: Count,
                           parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.count(parsedChildren.head))
  }

  protected def visitCountDistinct(expr: CountDistinct,
                           parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.countDistinct(parsedChildren.head, parsedChildren.tail: _*))
  }

  protected def visitCovPopulation(expr: CovPopulation,
                                   parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.covar_pop(parsedChildren.head, parsedChildren.last))
  }

  protected def visitCovSample(expr: CovSample,
                               parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.covar_samp(parsedChildren.head, parsedChildren.last))
  }

  protected def visitFirst(expr: First,
                               parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.first(parsedChildren.head, expr.ignoreNulls))
  }

  protected def visitGrouping(expr: Grouping,
                              parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.grouping(parsedChildren.head))
  }

  protected def visitGroupingID(expr: GroupingID,
                                parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.grouping_id(parsedChildren: _*))
  }

  protected def visitKurtosis(expr: Kurtosis,
                              parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.kurtosis(parsedChildren.head))
  }

  protected def visitLast(expr: Last,
                          parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.last(parsedChildren.head, expr.ignoreNulls))
  }

  protected def visitMax(expr: Max,
                          parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.max(parsedChildren.head))
  }

  protected def visitMin(expr: Min,
                         parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.min(parsedChildren.head))
  }

  protected def visitSkewness(expr: Skewness,
                              parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.skewness(parsedChildren.head))
  }

  protected def visitStddevSamp(expr: StddevSamp,
                              parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.stddev_samp(parsedChildren.head))
  }

  protected def visitStddevPop(expr: StddevPop,
                              parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.stddev_pop(parsedChildren.head))
  }

  protected def visitSum(expr: Sum,
                         parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    if (expr.isDistinct) {
      Some(sparkFunctions.sumDistinct(parsedChildren.head))
    } else {
      Some(sparkFunctions.sum(parsedChildren.head))
    }
  }

  protected def visitVarianceSamp(expr: VarianceSamp,
                                  parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.var_samp(parsedChildren.head))
  }

  protected def visitVariancePop(expr: VariancePop,
                                  parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.var_pop(parsedChildren.head))
  }
}
