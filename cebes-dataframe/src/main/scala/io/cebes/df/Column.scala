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
 * Created by phvu on 26/09/16.
 */

package io.cebes.df

import com.typesafe.scalalogging.LazyLogging
import io.cebes.df.expressions._
import io.cebes.df.types.storage.StorageType

/**
  * Represent a column of a [[io.cebes.df.Dataframe]], backed by an expression.
  *
  * @param expr Expression behind this column
  */
class Column(val expr: Expression) extends LazyLogging {

  /////////////////////////////////////////////////////////////////////////////
  // Private helpers
  /////////////////////////////////////////////////////////////////////////////

  /** Creates a column based on the given expression. */
  @inline private def withExpr(newExpr: Expression): Column = new Column(newExpr)

  /////////////////////////////////////////////////////////////////////////////
  //
  /////////////////////////////////////////////////////////////////////////////

  /**
    * Returns an ordering used in sorting.
    * {{{
    *   df.sort(df("col1").desc)
    * }}}
    */
  def desc: Column = withExpr {
    SortOrder(expr, Descending)
  }

  /**
    * Returns an ordering used in sorting.
    * {{{
    *   df.sort(df("col2").asc)
    * }}}
    */
  def asc: Column = withExpr {
    SortOrder(expr, Ascending)
  }

  /**
    * Unary minus, i.e. negate the expression.
    * {{{
    *   df.select( -df("cost") )
    * }}}
    */
  def unary_- : Column = withExpr {
    UnaryMinus(expr)
  }

  /**
    * Inversion of boolean expression, i.e. NOT.
    * {{{
    *   // select rows that are not rich (isRich === false)
    *   df.filter( !df("isRich") )
    * }}}
    */
  def unary_! : Column = withExpr {
    Not(expr)
  }

  /**
    * Equality test.
    * {{{
    *   df.filter( df("colA") === df("colB") )
    * }}}
    */
  def ===(other: Any): Column = withExpr {
    EqualTo(expr, functions.lit(other).expr)
  }

  /**
    * Equality test.
    * {{{
    *   df.filter( df("colA") === df("colB") )
    *   df.filter( df("colA").equalTo(df("colB")) )
    * }}}
    */
  def equalTo(other: Any): Column = this === other

  /**
    * Inequality test.
    * {{{
    *   df.select( df("colA") =!= df("colB") )
    *   df.filter( df("colA").notEqual(df("colB")) )
    * }}}
    */
  def =!=(other: Any): Column = withExpr {
    Not(EqualTo(expr, functions.lit(other).expr))
  }

  /**
    * Inequality test.
    * {{{
    *   df.select( df("colA") =!= df("colB") )
    *   df.filter( df("colA").notEqual(df("colB")) );
    * }}}
    */
  def notEqual(other: Any): Column = withExpr {
    Not(EqualTo(expr, functions.lit(other).expr))
  }

  /**
    * Greater than.
    * {{{
    *   // The following selects people older than 25.
    *   people.select( people("age") > 25 )
    *   people.select( people("age").gt(25) )
    * }}}
    */
  def >(other: Any): Column = withExpr {
    GreaterThan(expr, functions.lit(other).expr)
  }

  /**
    * Greater than.
    * {{{
    *   // Selects people older than 25.
    *   people.select( people("age") > 25 )
    *   people.select( people("age").gt(25) )
    * }}}
    */
  def gt(other: Any): Column = this > other

  /**
    * Less than.
    * {{{
    *   // Selects people younger than 27.
    *   people.select( people("age") < 27 )
    *   people.select( people("age").lt(27) )
    * }}}
    */
  def <(other: Any): Column = withExpr {
    LessThan(expr, functions.lit(other).expr)
  }

  /**
    * Less than.
    * {{{
    *   // Selects people younger than 27.
    *   people.select( people("age") < 27 )
    *   people.select( people("age").lt(27) )
    * }}}
    */
  def lt(other: Any): Column = this < other

  /**
    * Less than or equal to.
    * {{{
    *   // Selects people age 21 or younger than 21.
    *   people.select( people("age") <= 21 )
    *   people.select( people("age").leq(21) )
    * }}}
    */
  def <=(other: Any): Column = withExpr {
    LessThanOrEqual(expr, functions.lit(other).expr)
  }

  /**
    * Less than or equal to.
    * {{{
    *   // Selects people age 21 or younger than 21.
    *   people.select( people("age") <= 21 )
    *   people.select( people("age").leq(21) )
    * }}}
    */
  def leq(other: Any): Column = this <= other

  /**
    * Greater than or equal to.
    * {{{
    *   // Selects people age 21 or older than 21.
    *   people.select( people("age") >= 21 )
    *   people.select( people("age").geq(21) )
    * }}}
    */
  def >=(other: Any): Column = withExpr {
    GreaterThanOrEqual(expr, functions.lit(other).expr)
  }

  /**
    * Greater than or equal to.
    * {{{
    *   // Selects people age 21 or older than 21.
    *   people.select( people("age") >= 21 )
    *   people.select( people("age").geq(21) )
    * }}}
    */
  def geq(other: Any): Column = this >= other

  /**
    * Equality test that is safe for null values.
    * Returns same result with EQUAL(=) operator for non-null operands,
    * but returns TRUE if both are NULL, FALSE if one of the them is NULL.
    */
  def <=>(other: Any): Column = withExpr {
    EqualNullSafe(expr, functions.lit(other).expr)
  }

  /**
    * Equality test that is safe for null values.
    */
  def eqNullSafe(other: Any): Column = this <=> other

  /**
    * Evaluates a list of conditions and returns one of multiple possible result expressions.
    * If otherwise() is not defined at the end, `null` is returned for unmatched conditions.
    *
    * {{{
    *   // Example: encoding gender string column into integer.
    *
    *   people.select(when(people("gender") === "male", 0)
    *     .when(people("gender") === "female", 1)
    *     .otherwise(2))
    *
    *   people.select(when(people("gender").equalTo("male"), 0)
    *     .when(people("gender").equalTo("female"), 1)
    *     .otherwise(2))
    * }}}
    */
  def when(condition: Column, value: Any): Column = this.expr match {
    case CaseWhen(branches, None) =>
      withExpr {
        CaseWhen(branches :+ (condition.expr, functions.lit(value).expr))
      }
    case CaseWhen(branches, Some(_)) =>
      throw new IllegalArgumentException("when() cannot be applied once otherwise() is applied")
    case _ =>
      throw new IllegalArgumentException(
        "when() can only be applied on a Column previously generated by when() function")
  }

  /**
    * Evaluates a list of conditions and returns one of multiple possible result expressions.
    * If otherwise() is not defined at the end, `null` is returned for unmatched conditions.
    *
    * {{{
    *   // Example: encoding gender string column into integer.
    *
    *   people.select(when(people("gender") === "male", 0)
    *     .when(people("gender") === "female", 1)
    *     .otherwise(2))
    *
    *   people.select(when(people("gender").equalTo("male"), 0)
    *     .when(people("gender").equalTo("female"), 1)
    *     .otherwise(2))
    * }}}
    */
  def otherwise(value: Any): Column = this.expr match {
    case CaseWhen(branches, None) =>
      withExpr {
        CaseWhen(branches, Option(functions.lit(value).expr))
      }
    case CaseWhen(branches, Some(_)) =>
      throw new IllegalArgumentException(
        "otherwise() can only be applied once on a Column previously generated by when()")
    case _ =>
      throw new IllegalArgumentException(
        "otherwise() can only be applied on a Column previously generated by when()")
  }

  /**
    * True if the current column is between the lower bound and upper bound, inclusive.
    */
  def between(lowerBound: Any, upperBound: Any): Column = {
    (this >= lowerBound) && (this <= upperBound)
  }

  /**
    * True if the current expression is NaN.
    */
  def isNaN: Column = withExpr {
    IsNaN(expr)
  }

  /**
    * True if the current expression is null.
    */
  def isNull: Column = withExpr {
    IsNull(expr)
  }

  /**
    * True if the current expression is NOT null.
    */
  def isNotNull: Column = withExpr {
    IsNotNull(expr)
  }

  /**
    * Boolean OR.
    * {{{
    *   // Selects people that are in school or employed.
    *   people.filter( people("inSchool") || people("isEmployed") )
    *   people.filter( people("inSchool").or(people("isEmployed")) )
    * }}}
    */
  def ||(other: Any): Column = withExpr {
    Or(expr, functions.lit(other).expr)
  }

  /**
    * Boolean OR.
    * {{{
    *   // Selects people that are in school or employed.
    *   people.filter( people("inSchool") || people("isEmployed") )
    *   people.filter( people("inSchool").or(people("isEmployed")) )
    * }}}
    */
  def or(other: Column): Column = this || other

  /**
    * Boolean AND.
    * {{{
    *   // Selects people that are in school and employed at the same time.
    *   people.select( people("inSchool") && people("isEmployed") )
    *   people.select( people("inSchool").and(people("isEmployed")) )
    * }}}
    */
  def &&(other: Any): Column = withExpr {
    And(expr, functions.lit(other).expr)
  }

  /**
    * Boolean AND.
    * {{{
    *   // Selects people that are in school and employed at the same time.
    *   people.select( people("inSchool") && people("isEmployed") )
    *   people.select( people("inSchool").and(people("isEmployed")) )
    * }}}
    */
  def and(other: Column): Column = this && other

  /**
    * Sum of this expression and another expression. Only work on numeric columns.
    * {{{
    *   // Selects the sum of a person's height and weight.
    *   people.select( people("height") + people("weight") )
    *   people.select( people("height").plus(people("weight")) )
    * }}}
    */
  def +(other: Any): Column = withExpr {
    Add(expr, functions.lit(other).expr)
  }

  /**
    * Sum of this expression and another expression. Only work on numeric columns.
    * {{{
    *   // Selects the sum of a person's height and weight.
    *   people.select( people("height") + people("weight") )
    *   people.select( people("height").plus(people("weight")) )
    * }}}
    */
  def plus(other: Any): Column = this + other

  /**
    * Subtraction. Subtract the other expression from this expression.
    * Only work on numeric columns.
    * {{{
    *   // Selects the difference between people's height and their weight.
    *   people.select( people("height") - people("weight") )
    *   people.select( people("height").minus(people("weight")) )
    * }}}
    */
  def -(other: Any): Column = withExpr {
    Subtract(expr, functions.lit(other).expr)
  }

  /**
    * Subtraction. Subtract the other expression from this expression.
    * Only work on numeric columns.
    * {{{
    *   // Selects the difference between people's height and their weight.
    *   people.select( people("height") - people("weight") )
    *   people.select( people("height").minus(people("weight")) )
    * }}}
    */
  def minus(other: Any): Column = this - other

  /**
    * Multiplication of this expression and another expression.
    * Only work on numeric columns.
    * {{{
    *   // Multiplies a person's height by their weight.
    *   people.select( people("height") * people("weight") )
    *   people.select( people("height").multiply(people("weight")) )
    * }}}
    */
  def *(other: Any): Column = withExpr {
    Multiply(expr, functions.lit(other).expr)
  }

  /**
    * Multiplication of this expression and another expression.
    * Only work on numeric columns.
    * {{{
    *   // Multiplies a person's height by their weight.
    *   people.select( people("height") * people("weight") )
    *   people.select( people("height").multiply(people("weight")) )
    * }}}
    */
  def multiply(other: Any): Column = this * other

  /**
    * Division this expression by another expression.
    * Only work on numeric columns.
    * {{{
    *   // Divides a person's height by their weight.
    *   people.select( people("height") / people("weight") )
    *   people.select( people("height").divide(people("weight")) )
    * }}}
    */
  def /(other: Any): Column = withExpr {
    Divide(expr, functions.lit(other).expr)
  }

  /**
    * Division this expression by another expression.
    * Only work on numeric columns.
    * {{{
    *   // Divides a person's height by their weight.
    *   people.select( people("height") / people("weight") )
    *   people.select( people("height").divide(people("weight")) )
    * }}}
    */
  def divide(other: Any): Column = this / other

  /**
    * Modulo (a.k.a. remainder) expression.
    */
  def %(other: Any): Column = withExpr {
    Remainder(expr, functions.lit(other).expr)
  }

  /**
    * Modulo (a.k.a. remainder) expression.
    */
  def mod(other: Any): Column = this % other

  /**
    * A boolean expression that is evaluated to true if the value of this expression is contained
    * by the evaluated values of the arguments.
    */
  def isin(list: Any*): Column = withExpr {
    In(expr, list.map(functions.lit(_).expr))
  }

  /**
    * SQL like expression.
    * Result will be a BooleanType column
    */
  def like(literal: String): Column = withExpr {
    Like(expr, literal)
  }

  /**
    * SQL RLIKE expression (LIKE with Regex).
    * Result will be a BooleanType column
    */
  def rlike(literal: String): Column = withExpr {
    RLike(expr, literal)
  }

  /**
    * An expression that gets an item at position `ordinal` out of an array,
    * or gets a value by key `key` in a [[io.cebes.df.types.storage.MapType]].
    */
  def getItem(key: Any): Column = withExpr {
    GetItem(expr, functions.lit(key).expr)
  }

  /**
    * An expression that gets a field by name in a [[io.cebes.df.types.storage.StructType]].
    */
  def getField(fieldName: String): Column = withExpr {
    GetField(expr, fieldName)
  }

  /**
    * An expression that returns a substring.
    * NOTE: that this is not zero based, but 1-based index. The first character in str has index 1.
    *
    * `startPos` and `len` are handled specially:
    * - `"Content".substr(1, 3)` gives `"Con"`
    * - `"Content".substr(-100, 2)` gives `""`
    * - `"Content".substr(-100, 102)` gives `"Content"`
    * - `"Content".substr(2, 100)` gives `"ontent"`
    *
    * @param startPos expression for the starting position.
    * @param len      expression for the length of the substring.
    */
  def substr(startPos: Column, len: Column): Column = withExpr {
    Substring(expr, startPos.expr, len.expr)
  }

  /**
    * An expression that returns a substring.
    * NOTE: that this is not zero based, but 1-based index. The first character in str has index 1.
    *
    * `startPos` and `len` are handled specially:
    * - `"Content".substr(1, 3)` gives `"Con"`
    * - `"Content".substr(-100, 2)` gives `""`
    * - `"Content".substr(-100, 102)` gives `"Content"`
    * - `"Content".substr(2, 100)` gives `"ontent"`
    *
    * @param startPos starting position.
    * @param len      length of the substring.
    */
  def substr(startPos: Int, len: Int): Column = withExpr {
    Substring(expr, functions.lit(startPos).expr, functions.lit(len).expr)
  }

  /**
    * Contains the other element.
    */
  def contains(other: Any): Column = withExpr {
    Contains(expr, functions.lit(other).expr)
  }

  /**
    * String starts with.
    */
  def startsWith(other: Column): Column = withExpr {
    StartsWith(expr, functions.lit(other).expr)
  }

  /**
    * String starts with another string literal.
    */
  def startsWith(literal: String): Column = this.startsWith(functions.lit(literal))

  /**
    * String ends with.
    */
  def endsWith(other: Column): Column = withExpr {
    EndsWith(expr, functions.lit(other).expr)
  }

  /**
    * String ends with another string literal.
    */
  def endsWith(literal: String): Column = this.endsWith(functions.lit(literal))

  /**
    * Gives the column an alias. Same as `as`.
    * {{{
    *   // Renames colA to colB in select output.
    *   df.select(df("colA").alias("colB"))
    * }}}
    */
  def alias(alias: String): Column = name(alias)

  /**
    * Gives the column an alias.
    * {{{
    *   // Renames colA to colB in select output.
    *   df.select(df("colA").as("colB"))
    * }}}
    *
    * If the current column has metadata associated with it, this metadata will be propagated
    * to the new column.  If this not desired, use `as` with explicitly empty metadata.
    */
  def as(alias: String): Column = name(alias)

  /**
    * (Scala-specific) Assigns the given aliases to the results of a table generating function.
    * {{{
    *   // Renames colA to colB in select output.
    *   df.select(explode($"myMap").as("key" :: "value" :: Nil))
    * }}}
    */
  def as(aliases: Seq[String]): Column = withExpr {
    MultiAlias(expr, aliases)
  }

  /**
    * Assigns the given aliases to the results of a table generating function.
    * {{{
    *   // Renames colA to colB in select output.
    *   df.select(explode(df("myMap")).as("key" :: "value" :: Nil))
    * }}}
    *
    * @group expr_ops
    * @since 1.4.0
    */
  def as(aliases: Array[String]): Column = withExpr {
    MultiAlias(expr, aliases)
  }

  /**
    * Gives the column an alias.
    * {{{
    *   // Renames colA to colB in select output.
    *   df.select(df("colA").as('colB))
    * }}}
    */
  def as(alias: Symbol): Column = name(alias.name)

  /**
    * Gives the column a name (alias).
    * {{{
    *   // Renames colA to colB in select output.
    *   df.select(df("colA").name("colB"))
    * }}}
    *
    * If the current column has metadata associated with it, this metadata will be propagated
    * to the new column.  If this not desired, use `as` with explicitly empty metadata.
    */
  def name(alias: String): Column = withExpr {
    Alias(expr, alias)
  }

  /**
    * Casts the column to a different data type.
    * {{{
    *   // Casts colA to [[IntegerType]].
    *   import io.cebes.df.types.storage.IntegerType
    *   df.select(df("colA").cast(IntegerType))
    *   df.select(df("colA").cast("int"))
    * }}}
    */
  def cast(to: StorageType): Column = withExpr {
    Cast(expr, to)
  }

  /**
    * Compute bitwise OR of this expression with another expression.
    * {{{
    *   df.select(df("colA").bitwiseOR(df("colB")))
    * }}}
    */
  def bitwiseOR(other: Any): Column = withExpr {
    BitwiseOr(expr, functions.lit(other).expr)
  }

  /**
    * Compute bitwise AND of this expression with another expression.
    * {{{
    *   df.select(df("colA").bitwiseAND(df("colB")))
    * }}}
    */
  def bitwiseAND(other: Any): Column = withExpr {
    BitwiseAnd(expr, functions.lit(other).expr)
  }

  /**
    * Compute bitwise XOR of this expression with another expression.
    * {{{
    *   df.select(df("colA").bitwiseXOR(df("colB")))
    * }}}
    */
  def bitwiseXOR(other: Any): Column = withExpr {
    BitwiseXor(expr, functions.lit(other).expr)
  }

  /**
    * Computes bitwise NOT.
    * {{{
    *   df.select(df("colA").bitwiseNOT)
    *   df.select(bitwiseNOT(df("colA"))
    * }}}
    */
  def bitwiseNOT: Column = withExpr { BitwiseNot(expr) }

  /**
    * Define a windowing column.
    *
    * {{{
    *   val w = Window.partitionBy("name").orderBy("id")
    *   df.select(
    *     sum("price").over(w.rangeBetween(Long.MinValue, 2)),
    *     avg("price").over(w.rowsBetween(0, 4))
    *   )
    * }}}
    */
  //TODO: implement over() once we figured out the aggregation properly
  //def over(window: expressions.WindowSpec): Column = window.withAggregate(this)

  /**
    * Define a empty analytic clause. In this case the analytic function is applied
    * and presented for all rows in the result set.
    *
    * {{{
    *   df.select(
    *     sum("price").over(),
    *     avg("price").over()
    *   )
    * }}}
    */
  //def over(): Column = over(Window.spec)
}
