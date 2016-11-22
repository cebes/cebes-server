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
 * Created by phvu on 22/11/2016.
 */

package io.cebes.df

/**
  * A set of methods for aggregations on a [[Dataframe]], created by [[Dataframe.groupBy]].
  */
trait GroupedDataframe {

  /**
    * Compute aggregates by specifying a map from column name to
    * aggregate methods. The resulting [[Dataframe]] will also contain the grouping columns.
    *
    * The available aggregate methods are `avg`, `max`, `min`, `sum`, `count`.
    * {{{
    *   // Selects the age of the oldest employee and the aggregate expense for each department
    *   df.groupBy("department").agg(
    *     "age" -> "max",
    *     "expense" -> "sum"
    *   )
    * }}}
    */
  def agg(aggExpr: (String, String), aggExprs: (String, String)*): Dataframe = {
    agg((aggExpr +: aggExprs).toMap)
  }

  /**
    * (Scala-specific) Compute aggregates by specifying a map from column name to
    * aggregate methods. The resulting [[Dataframe]] will also contain the grouping columns.
    *
    * The available aggregate methods are `avg`, `max`, `min`, `sum`, `count`.
    * {{{
    *   // Selects the age of the oldest employee and the aggregate expense for each department
    *   df.groupBy("department").agg(Map(
    *     "age" -> "max",
    *     "expense" -> "sum"
    *   ))
    * }}}
    *
    * @since 1.3.0
    */
  def agg(exprs: Map[String, String]): Dataframe

  /**
    * Compute aggregates by specifying a series of aggregate columns. Note that this function by
    * default retains the grouping columns in its output. To not retain grouping columns, set
    * `spark.sql.retainGroupColumns` to false.
    *
    * The available aggregate methods are defined in [[functions]].
    *
    * {{{
    *   // Selects the age of the oldest employee and the aggregate expense for each department
    *
    *   // Scala:
    *   import org.apache.spark.sql.functions._
    *   df.groupBy("department").agg(max("age"), sum("expense"))
    *
    *   // Java:
    *   import static org.apache.spark.sql.functions.*;
    *   df.groupBy("department").agg(max("age"), sum("expense"));
    * }}}
    */
  def agg(expr: Column, exprs: Column*): Dataframe

  /**
    * Count the number of rows for each group.
    * The resulting [[Dataframe]] will also contain the grouping columns.
    */
  def count(): Dataframe

  /**
    * Compute the average value for each numeric columns for each group. This is an alias for `avg`.
    * The resulting [[Dataframe]] will also contain the grouping columns.
    * When specified columns are given, only compute the average values for them.
    */
  def mean(colNames: String*): Dataframe = avg(colNames: _*)

  /**
    * Compute the max value for each numeric columns for each group.
    * The resulting [[Dataframe]] will also contain the grouping columns.
    * When specified columns are given, only compute the max values for them.
    */
  def max(colNames: String*): Dataframe

  /**
    * Compute the mean value for each numeric columns for each group.
    * The resulting [[Dataframe]] will also contain the grouping columns.
    * When specified columns are given, only compute the mean values for them.
    *
    */
  def avg(colNames: String*): Dataframe

  /**
    * Compute the min value for each numeric column for each group.
    * The resulting [[Dataframe]] will also contain the grouping columns.
    * When specified columns are given, only compute the min values for them.
    */
  def min(colNames: String*): Dataframe

  /**
    * Compute the sum for each numeric columns for each group.
    * The resulting [[Dataframe]] will also contain the grouping columns.
    * When specified columns are given, only compute the sum for them.
    */
  def sum(colNames: String*): Dataframe

  /**
    * Pivots a column of the current [[Dataframe]] and perform the specified aggregation.
    * There are two versions of pivot function: one that requires the caller to specify the list
    * of distinct values to pivot on, and one that does not. The latter is more concise but less
    * efficient, because Spark needs to first compute the list of distinct values internally.
    *
    * {{{
    *   // Compute the sum of earnings for each year by course with each course as a separate column
    *   df.groupBy("year").pivot("course", Seq("dotNET", "Java")).sum("earnings")
    *
    *   // Or without specifying column values (less efficient)
    *   df.groupBy("year").pivot("course").sum("earnings")
    * }}}
    *
    * @param pivotColumn Name of the column to pivot.
    */
  def pivot(pivotColumn: String): GroupedDataframe

  /**
    * Pivots a column of the current [[Dataframe]] and perform the specified aggregation.
    * There are two versions of pivot function: one that requires the caller to specify the list
    * of distinct values to pivot on, and one that does not. The latter is more concise but less
    * efficient, because we need to first compute the list of distinct values internally.
    *
    * {{{
    *   // Compute the sum of earnings for each year by course with each course as a separate column
    *   df.groupBy("year").pivot("course", Seq("dotNET", "Java")).sum("earnings")
    *
    *   // Or without specifying column values (less efficient)
    *   df.groupBy("year").pivot("course").sum("earnings")
    * }}}
    *
    * @param pivotColumn Name of the column to pivot.
    * @param values List of values that will be translated to columns in the output Dataframe.
    */
  def pivot(pivotColumn: String, values: Seq[Any]): GroupedDataframe

}
