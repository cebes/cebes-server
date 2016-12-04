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

case class UnaryMinus(child: Expression) extends UnaryExpression

case class Add(left: Expression, right: Expression) extends BinaryExpression

case class Subtract(left: Expression, right: Expression) extends BinaryExpression

case class Multiply(left: Expression, right: Expression) extends BinaryExpression

case class Divide(left: Expression, right: Expression) extends BinaryExpression

case class Remainder(left: Expression, right: Expression) extends BinaryExpression

case class Abs(child: Expression) extends UnaryExpression

case class Sqrt(child: Expression) extends UnaryExpression

case class Acos(child: Expression) extends UnaryExpression

case class Asin(child: Expression) extends UnaryExpression

case class Atan(child: Expression) extends UnaryExpression

case class Atan2(left: Expression, right: Expression) extends BinaryExpression

case class Bin(child: Expression) extends UnaryExpression

case class Cbrt(child: Expression) extends UnaryExpression

case class Ceil(child: Expression) extends UnaryExpression

case class Conv(numExpr: Expression, fromBaseExpr: Expression, toBaseExpr: Expression) extends Expression {
  override def children = Seq(numExpr, fromBaseExpr, toBaseExpr)
}

case class Cos(child: Expression) extends UnaryExpression

case class Cosh(child: Expression) extends UnaryExpression

case class Exp(child: Expression) extends UnaryExpression

case class Expm1(child: Expression) extends UnaryExpression

case class Factorial(child: Expression) extends UnaryExpression

case class Floor(child: Expression) extends UnaryExpression

case class Greatest(children: Seq[Expression]) extends Expression

case class Hex(child: Expression) extends UnaryExpression

case class Unhex(child: Expression) extends UnaryExpression

case class Hypot(left: Expression, right: Expression) extends BinaryExpression

case class Least(children: Seq[Expression]) extends Expression

case class Log(child: Expression) extends UnaryExpression

case class Logarithm(left: Expression, right: Expression) extends BinaryExpression

case class Log10(child: Expression) extends UnaryExpression

case class Log1p(child: Expression) extends UnaryExpression

case class Log2(child: Expression) extends UnaryExpression

case class Pow(left: Expression, right: Expression) extends BinaryExpression

case class Pmod(left: Expression, right: Expression) extends BinaryExpression

case class Rint(child: Expression) extends UnaryExpression

case class Round(left: Expression, right: Expression) extends BinaryExpression

case class BRound(left: Expression, right: Expression) extends BinaryExpression

case class ShiftLeft(left: Expression, right: Expression) extends BinaryExpression

case class ShiftRight(left: Expression, right: Expression) extends BinaryExpression

case class ShiftRightUnsigned(left: Expression, right: Expression) extends BinaryExpression

case class Signum(child: Expression) extends UnaryExpression

case class Sin(child: Expression) extends UnaryExpression

case class Sinh(child: Expression) extends UnaryExpression

case class Tan(child: Expression) extends UnaryExpression

case class Tanh(child: Expression) extends UnaryExpression

case class ToDegrees(child: Expression) extends UnaryExpression

case class ToRadians(child: Expression) extends UnaryExpression
