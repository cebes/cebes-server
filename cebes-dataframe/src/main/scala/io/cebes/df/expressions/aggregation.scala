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
 * Created by phvu on 23/11/2016.
 */

package io.cebes.df.expressions

case class ApproxCountDistinct(child: Expression, relativeSD: Double = 0.05) extends UnaryExpression

case class Average(child: Expression) extends UnaryExpression

case class CollectList(child: Expression) extends UnaryExpression

case class CollectSet(child: Expression) extends UnaryExpression

case class Corr(left: Expression, right: Expression) extends BinaryExpression

case class Count(child: Expression) extends UnaryExpression

case class CountDistinct(expr: Expression, exprs: Expression*) extends Expression {
  override def children: Seq[Expression] = expr +: exprs
}

case class CovPopulation(left: Expression, right: Expression) extends BinaryExpression

case class CovSample(left: Expression, right: Expression) extends BinaryExpression

case class First(child: Expression, ignoreNulls: Boolean) extends UnaryExpression

case class Grouping(child: Expression) extends UnaryExpression

case class GroupingID(children: Seq[Expression]) extends Expression

case class Kurtosis(child: Expression) extends UnaryExpression

case class Last(child: Expression, ignoreNulls: Boolean) extends UnaryExpression

case class Max(child: Expression) extends UnaryExpression

case class Min(child: Expression) extends UnaryExpression

case class Skewness(child: Expression) extends UnaryExpression

case class StddevSamp(child: Expression) extends UnaryExpression

case class StddevPop(child: Expression) extends UnaryExpression

case class Sum(child: Expression, isDistinct: Boolean = false) extends UnaryExpression

case class VarianceSamp(child: Expression) extends UnaryExpression

case class VariancePop(child: Expression) extends UnaryExpression
