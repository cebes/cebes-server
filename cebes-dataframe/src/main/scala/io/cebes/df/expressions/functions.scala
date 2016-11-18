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

import io.cebes.df.Column

object functions {

  private def withExpr(expr: Expression): Column = new Column(expr)

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
      case s: Symbol => new Column(Literal(literal))
      case _ => new Column(Literal(literal))
    }
  }

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
