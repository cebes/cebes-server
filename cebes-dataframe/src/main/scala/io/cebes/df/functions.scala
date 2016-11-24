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
 * Created by phvu on 17/11/2016.
 */

package io.cebes.df

import io.cebes.df.expressions._

object functions {

  private def withExpr(expr: Expression): Column = new Column(expr)

  /**
    * Returns a [[Column]] based on the given column name.
    */
  def col(colName: String): Column = new Column(UnresolvedColumnName(colName))

  /**
    * Creates a [[io.cebes.df.Column]] of literal value.
    *
    * The passed in object is returned directly if it is already a [[io.cebes.df.Column]].
    * If the object is a Scala Symbol, it is converted into a [[io.cebes.df.Column]] also.
    * Otherwise, a new [[io.cebes.df.Column]] is created to represent the literal value.
    */
  def lit(literal: Any): Column = {
    literal match {
      case c: Column => c
      case _ => new Column(Literal(literal))
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Aggregate functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
    * Aggregate function: returns the approximate number of distinct items in a group.
    *
    * @group agg_funcs
    */
  def approxCountDistinct(e: Column): Column = withExpr {
    ApproxCountDistinct(e.expr)
  }

  /**
    * Aggregate function: returns the approximate number of distinct items in a group.
    *
    * @group agg_funcs
    */
  def approxCountDistinct(columnName: String): Column = approxCountDistinct(col(columnName))

  /**
    * Aggregate function: returns the approximate number of distinct items in a group.
    *
    * @param rsd maximum estimation error allowed (default = 0.05)
    *
    * @group agg_funcs
    */
  def approxCountDistinct(e: Column, rsd: Double): Column = withExpr {
    ApproxCountDistinct(e.expr, rsd)
  }

  /**
    * Aggregate function: returns the approximate number of distinct items in a group.
    *
    * @param rsd maximum estimation error allowed (default = 0.05)
    *
    * @group agg_funcs
    */
  def approxCountDistinct(columnName: String, rsd: Double): Column = {
    approxCountDistinct(col(columnName), rsd)
  }

  /**
    * Aggregate function: returns the average of the values in a group.
    *
    * @group agg_funcs
    */
  def avg(e: Column): Column = withExpr { Average(e.expr) }

  /**
    * Aggregate function: returns the average of the values in a group.
    *
    * @group agg_funcs
    */
  def avg(columnName: String): Column = avg(col(columnName))

  /**
    * Aggregate function: returns a list of objects with duplicates.
    *
    * @group agg_funcs
    */
  def collectList(e: Column): Column = withExpr { CollectList(e.expr) }

  /**
    * Aggregate function: returns a list of objects with duplicates.
    *
    * @group agg_funcs
    */
  def collectList(columnName: String): Column = collectList(col(columnName))

  /**
    * Aggregate function: returns a set of objects with duplicate elements eliminated.
    *
    * @group agg_funcs
    */
  def collectSet(e: Column): Column = withExpr { CollectSet(e.expr) }

  /**
    * Aggregate function: returns a set of objects with duplicate elements eliminated.
    *
    * @group agg_funcs
    */
  def collectSet(columnName: String): Column = collectSet(col(columnName))

  /**
    * Aggregate function: returns the Pearson Correlation Coefficient for two columns.
    *
    * @group agg_funcs
    */
  def corr(column1: Column, column2: Column): Column = withExpr {
    Corr(column1.expr, column2.expr)
  }

  /**
    * Aggregate function: returns the Pearson Correlation Coefficient for two columns.
    *
    * @group agg_funcs
    */
  def corr(columnName1: String, columnName2: String): Column = {
    corr(col(columnName1), col(columnName2))
  }

  /**
    * Aggregate function: returns the number of items in a group.
    *
    * @group agg_funcs
    */
  def count(e: Column): Column = withExpr { Count(e.expr) }

  /**
    * Aggregate function: returns the number of items in a group.
    *
    * @group agg_funcs
    */
  def count(columnName: String): Column = count(col(columnName))

  /**
    * Aggregate function: returns the number of distinct items in a group.
    *
    * @group agg_funcs
    */
  def countDistinct(expr: Column, exprs: Column*): Column = withExpr {
    CountDistinct(expr.expr, exprs.map(_.expr): _*)
  }

  /**
    * Aggregate function: returns the number of distinct items in a group.
    *
    * @group agg_funcs
    */
  def countDistinct(columnName: String, columnNames: String*): Column =
  countDistinct(col(columnName), columnNames.map(col) : _*)

  /**
    * Aggregate function: returns the population covariance for two columns.
    *
    * @group agg_funcs
    */
  def covarPop(column1: Column, column2: Column): Column = withExpr {
    CovPopulation(column1.expr, column2.expr)
  }

  /**
    * Aggregate function: returns the population covariance for two columns.
    *
    * @group agg_funcs
    */
  def covarPop(columnName1: String, columnName2: String): Column = {
    covarPop(col(columnName1), col(columnName2))
  }

  /**
    * Aggregate function: returns the sample covariance for two columns.
    *
    * @group agg_funcs
    */
  def covarSamp(column1: Column, column2: Column): Column = withExpr {
    CovSample(column1.expr, column2.expr)
  }

  /**
    * Aggregate function: returns the sample covariance for two columns.
    *
    * @group agg_funcs
    */
  def covarSamp(columnName1: String, columnName2: String): Column = {
    covarSamp(col(columnName1), col(columnName2))
  }

  /**
    * Aggregate function: returns the first value in a group.
    *
    * The function by default returns the first values it sees. It will return the first non-null
    * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
    *
    * @group agg_funcs
    */
  def first(e: Column, ignoreNulls: Boolean): Column = withExpr {
    First(e.expr, ignoreNulls)
  }

  /**
    * Aggregate function: returns the first value of a column in a group.
    *
    * The function by default returns the first values it sees. It will return the first non-null
    * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
    *
    * @group agg_funcs
    */
  def first(columnName: String, ignoreNulls: Boolean): Column = {
    first(col(columnName), ignoreNulls)
  }

  /**
    * Aggregate function: returns the first value in a group.
    *
    * The function by default returns the first values it sees. It will return the first non-null
    * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
    *
    * @group agg_funcs
    */
  def first(e: Column): Column = first(e, ignoreNulls = false)

  /**
    * Aggregate function: returns the first value of a column in a group.
    *
    * The function by default returns the first values it sees. It will return the first non-null
    * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
    *
    * @group agg_funcs
    */
  def first(columnName: String): Column = first(col(columnName))

  /**
    * Aggregate function: indicates whether a specified column in a GROUP BY list is aggregated
    * or not, returns 1 for aggregated or 0 for not aggregated in the result set.
    *
    * @group agg_funcs
    */
  def grouping(e: Column): Column = withExpr(Grouping(e.expr))

  /**
    * Aggregate function: indicates whether a specified column in a GROUP BY list is aggregated
    * or not, returns 1 for aggregated or 0 for not aggregated in the result set.
    *
    * @group agg_funcs
    */
  def grouping(columnName: String): Column = grouping(col(columnName))

  /**
    * Aggregate function: returns the level of grouping, equals to
    *
    *   (grouping(c1) << (n-1)) + (grouping(c2) << (n-2)) + ... + grouping(cn)
    *
    * Note: the list of columns should match with grouping columns exactly, or empty (means all the
    * grouping columns).
    *
    * @group agg_funcs
    */
  def groupingId(cols: Column*): Column = withExpr(GroupingID(cols.map(_.expr): _*))

  /**
    * Aggregate function: returns the level of grouping, equals to
    *
    *   (grouping(c1) << (n-1)) + (grouping(c2) << (n-2)) + ... + grouping(cn)
    *
    * Note: the list of columns should match with grouping columns exactly.
    *
    * @group agg_funcs
    */
  def groupingId(colName: String, colNames: String*): Column = {
    groupingId((Seq(colName) ++ colNames).map(n => col(n)) : _*)
  }

  /**
    * Aggregate function: returns the kurtosis of the values in a group.
    *
    * @group agg_funcs
    */
  def kurtosis(e: Column): Column = withExpr { Kurtosis(e.expr) }

  /**
    * Aggregate function: returns the kurtosis of the values in a group.
    *
    * @group agg_funcs
    */
  def kurtosis(columnName: String): Column = kurtosis(col(columnName))

  /**
    * Aggregate function: returns the last value in a group.
    *
    * The function by default returns the last values it sees. It will return the last non-null
    * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
    *
    * @group agg_funcs
    */
  def last(e: Column, ignoreNulls: Boolean): Column = withExpr {
    Last(e.expr, ignoreNulls)
  }

  /**
    * Aggregate function: returns the last value of the column in a group.
    *
    * The function by default returns the last values it sees. It will return the last non-null
    * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
    *
    * @group agg_funcs
    */
  def last(columnName: String, ignoreNulls: Boolean): Column = {
    last(col(columnName), ignoreNulls)
  }

  /**
    * Aggregate function: returns the last value in a group.
    *
    * The function by default returns the last values it sees. It will return the last non-null
    * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
    *
    * @group agg_funcs
    */
  def last(e: Column): Column = last(e, ignoreNulls = false)

  /**
    * Aggregate function: returns the last value of the column in a group.
    *
    * The function by default returns the last values it sees. It will return the last non-null
    * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
    *
    * @group agg_funcs
    */
  def last(columnName: String): Column = last(col(columnName), ignoreNulls = false)

  /**
    * Aggregate function: returns the maximum value of the expression in a group.
    *
    * @group agg_funcs
    */
  def max(e: Column): Column = withExpr { Max(e.expr) }

  /**
    * Aggregate function: returns the maximum value of the column in a group.
    *
    * @group agg_funcs
    */
  def max(columnName: String): Column = max(col(columnName))

  /**
    * Aggregate function: returns the average of the values in a group.
    * Alias for avg.
    *
    * @group agg_funcs
    */
  def mean(e: Column): Column = avg(e)

  /**
    * Aggregate function: returns the average of the values in a group.
    * Alias for avg.
    *
    * @group agg_funcs
    */
  def mean(columnName: String): Column = avg(columnName)

  /**
    * Aggregate function: returns the minimum value of the expression in a group.
    *
    * @group agg_funcs
    */
  def min(e: Column): Column = withExpr { Min(e.expr) }

  /**
    * Aggregate function: returns the minimum value of the column in a group.
    *
    * @group agg_funcs
    */
  def min(columnName: String): Column = min(col(columnName))

  /**
    * Aggregate function: returns the skewness of the values in a group.
    *
    * @group agg_funcs
    */
  def skewness(e: Column): Column = withExpr { Skewness(e.expr) }

  /**
    * Aggregate function: returns the skewness of the values in a group.
    *
    * @group agg_funcs
    */
  def skewness(columnName: String): Column = skewness(col(columnName))

  /**
    * Aggregate function: alias for [[stddevSamp]].
    *
    * @group agg_funcs
    */
  def stddev(e: Column): Column = withExpr { StddevSamp(e.expr) }

  /**
    * Aggregate function: alias for [[stddevSamp]].
    *
    * @group agg_funcs
    */
  def stddev(columnName: String): Column = stddev(col(columnName))

  /**
    * Aggregate function: returns the sample standard deviation of
    * the expression in a group.
    *
    * @group agg_funcs
    */
  def stddevSamp(e: Column): Column = withExpr { StddevSamp(e.expr) }

  /**
    * Aggregate function: returns the sample standard deviation of
    * the expression in a group.
    *
    * @group agg_funcs
    */
  def stddevSamp(columnName: String): Column = stddevSamp(col(columnName))

  /**
    * Aggregate function: returns the population standard deviation of
    * the expression in a group.
    *
    * @group agg_funcs
    */
  def stddevPop(e: Column): Column = withExpr { StddevPop(e.expr) }

  /**
    * Aggregate function: returns the population standard deviation of
    * the expression in a group.
    *
    * @group agg_funcs
    */
  def stddevPop(columnName: String): Column = stddevPop(col(columnName))

  /**
    * Aggregate function: returns the sum of all values in the expression.
    *
    * @group agg_funcs
    */
  def sum(e: Column): Column = withExpr { Sum(e.expr) }

  /**
    * Aggregate function: returns the sum of all values in the given column.
    *
    * @group agg_funcs
    */
  def sum(columnName: String): Column = sum(col(columnName))

  /**
    * Aggregate function: returns the sum of distinct values in the expression.
    *
    * @group agg_funcs
    */
  def sumDistinct(e: Column): Column = withExpr(Sum(e.expr, isDistinct = true))

  /**
    * Aggregate function: returns the sum of distinct values in the expression.
    *
    * @group agg_funcs
    */
  def sumDistinct(columnName: String): Column = sumDistinct(col(columnName))

  /**
    * Aggregate function: alias for [[varSamp]].
    *
    * @group agg_funcs
    */
  def variance(e: Column): Column = withExpr { VarianceSamp(e.expr) }

  /**
    * Aggregate function: alias for [[varSamp]].
    *
    * @group agg_funcs
    */
  def variance(columnName: String): Column = variance(col(columnName))

  /**
    * Aggregate function: returns the unbiased variance of the values in a group.
    *
    * @group agg_funcs
    */
  def varSamp(e: Column): Column = withExpr { VarianceSamp(e.expr) }

  /**
    * Aggregate function: returns the unbiased variance of the values in a group.
    *
    * @group agg_funcs
    */
  def varSamp(columnName: String): Column = varSamp(col(columnName))

  /**
    * Aggregate function: returns the population variance of the values in a group.
    *
    * @group agg_funcs
    */
  def varPop(e: Column): Column = withExpr { VariancePop(e.expr) }

  /**
    * Aggregate function: returns the population variance of the values in a group.
    *
    * @group agg_funcs
    */
  def varPop(columnName: String): Column = varPop(col(columnName))


  //////////////////////////////////////////////////////////////////////////////////////////////
  // Non-aggregate functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
    * Evaluates a list of conditions and returns one of multiple possible result expressions.
    * If otherwise is not defined at the end, null is returned for unmatched conditions.
    *
    * {{{
    *   // Example: encoding gender string column into integer.
    *   people.select(when(people("gender") === "male", 0)
    *     .when(people("gender") === "female", 1)
    *     .otherwise(2))
    *
    *   people.select(when(col("gender").equalTo("male"), 0)
    *     .when(col("gender").equalTo("female"), 1)
    *     .otherwise(2))
    * }}}
    */
  def when(condition: Column, value: Any): Column = withExpr {
    CaseWhen(Seq((condition.expr, lit(value).expr)))
  }
}
