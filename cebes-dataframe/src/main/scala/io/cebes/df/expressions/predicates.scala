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

package io.cebes.df.expressions

case class Not(child: Expression) extends UnaryExpression

case class EqualTo(left: Expression, right: Expression) extends BinaryExpression

case class GreaterThan(left: Expression, right: Expression) extends BinaryExpression

case class LessThan(left: Expression, right: Expression) extends BinaryExpression

case class GreaterThanOrEqual(left: Expression, right: Expression) extends BinaryExpression

case class LessThanOrEqual(left: Expression, right: Expression) extends BinaryExpression

case class EqualNullSafe(left: Expression, right: Expression) extends BinaryExpression

case class IsNaN(child: Expression) extends UnaryExpression

case class IsNull(child: Expression) extends UnaryExpression

case class IsNotNull(child: Expression) extends UnaryExpression

case class Or(left: Expression, right: Expression) extends BinaryExpression

case class And(left: Expression, right: Expression) extends BinaryExpression

case class In(value: Expression, list: Seq[Expression]) extends Expression {

  override def children: Seq[Expression] = value +: list
}

case class BitwiseOr(left: Expression, right: Expression) extends BinaryExpression

case class BitwiseAnd(left: Expression, right: Expression) extends BinaryExpression

case class BitwiseXor(left: Expression, right: Expression) extends BinaryExpression

case class BitwiseNot(child: Expression) extends UnaryExpression
