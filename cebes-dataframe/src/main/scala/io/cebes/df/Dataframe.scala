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
 * Created by phvu on 26/08/16.
 */

package io.cebes.df

import io.cebes.common.HasId
import io.cebes.df.sample.DataSample
import io.cebes.df.schema.{HasSchema, Schema}
import io.cebes.df.types.VariableTypes.VariableType

/**
  * Cebes Dataframe
  */
trait Dataframe extends HasSchema with HasId {

  /**
    * Number of rows
    */
  def numRows: Long

  /** Selects column based on the column name and return it as a [[Column]]. **/
  def apply(colName: String): Column = col(colName)

  /**
    * Automatically infer variable types, using various heuristics based on data
    *
    * @return a new [[Dataframe]]
    * @group Schema manipulation
    */
  def inferVariableTypes(sampleSize: Int = 1000): Dataframe

  /**
    * Manually update variable types for each column. Column names are case-insensitive.
    * Sanity checks will be performed. If new variable type doesn't conform with its storage type,
    * an exception will be thrown.
    *
    * @param newTypes map from column name -> new [[VariableType]]
    * @return a new [[Dataframe]]
    * @group Schema manipulation
    */
  def withVariableTypes(newTypes: Map[String, VariableType]): Dataframe

  /**
    * Manually update variable types for each column. Column names are case-insensitive.
    * Sanity checks will be performed. If new variable type doesn't conform with its storage type,
    * an exception will be thrown.
    *
    * @param colName column name
    * @param variableType new variable type
    * @return a new [[Dataframe]]
    * @group Schema manipulation
    */
  def withVariableType(colName: String, variableType: VariableType): Dataframe =
  withVariableTypes(Map(colName -> variableType))

  /**
    * Apply a new schema to this data frame
    *
    * @param newSchema the new Schema
    * @return a new dataframe with the new Schema
    */
  def applySchema(newSchema: Schema): Dataframe

  /**
    * Get the first n rows. If the [[Dataframe]] has less than n rows, all rows will be returned.
    * Since the data will be gathered to the memory of a single JVM process,
    * calling this function with big n might cause [[OutOfMemoryError]]
    *
    * @param n number of rows to take
    * @return a [[DataSample]] object containing the data.
    * @group Sampling functions
    */
  def take(n: Int = 1): DataSample

  /**
    * Randomly sample n rows, return the result as a [[Dataframe]]
    *
    * @param withReplacement Sample with replacement or not.
    * @param fraction        Fraction of rows to generate.
    * @param seed            Seed for sampling.
    * @return a [[Dataframe]] object containing the data.
    * @group Sampling functions
    */
  def sample(withReplacement: Boolean, fraction: Double, seed: Long): Dataframe

  /**
    * Create a temporary view of this Dataframe,
    * so you can run SQL commands against
    *
    * @param name name of the view
    */
  def createTempView(name: String)

  ////////////////////////////////////////////////////////////////////////////////////
  // Data exploration
  ////////////////////////////////////////////////////////////////////////////////////

  /**
    * Returns a new Dataframe sorted by the given expressions. This is an alias for `orderedBy`
    *
    * @group data-exploration
    */
  def sort(sortExprs: Column*): Dataframe

  /**
    * Returns a new Dataframe with columns dropped.
    * This is a no-op if schema doesn't contain column name(s).
    *
    * The colName string is treated literally without further interpretation.
    *
    * @group data-exploration
    */
  def drop(colNames: Seq[String]): Dataframe

  /**
    * Returns a new Dataframe that contains only the unique rows from this Dataframe.
    *
    * Note that, equality checking is performed directly on the encoded representation of the data
    * and thus is not affected by a custom `equals` function.
    *
    * @group data-exploration
    */
  def dropDuplicates(colNames: Seq[String]): Dataframe

  ////////////////////////////////////////////////////////////////////////////////////
  // SQL-related functions
  ////////////////////////////////////////////////////////////////////////////////////

  /**
    * Returns a new [[Dataframe]] by adding a column or replacing
    * the existing column that has the same name (case-insensitive).
    *
    * @group sql-api
    */
  def withColumn(colName: String, col: Column): Dataframe

  /**
    * Returns a new [[Dataframe]] with a column renamed.
    *
    * @group sql-api
    */
  def withColumnRenamed(existingName: String, newName: String): Dataframe

  /**
    * Selects a set of columns based on expressions.
    *
    * @group sql-api
    */
  def select(columns: Column*): Dataframe

  /**
    * Selects a set of columns. This is a variant of `select` that can only select
    * existing columns using column names (i.e. cannot construct expressions).
    *
    * {{{
    *   ds.select("colA", "colB")
    * }}}
    *
    * @group sql-api
    */
  def select(col: String, cols: String*): Dataframe

  /**
    * Filters rows using the given condition.
    *
    * @group sql-api
    */
  def where(column: Column): Dataframe

  /**
    * Returns a new Dataframe sorted by the given expressions. This is an alias for `sort`.
    *
    * @group sql-api
    */
  def orderBy(sortExprs: Column*): Dataframe = sort(sortExprs: _*)

  /**
    * Selects column based on the column name and return it as a [[Column]].
    *
    * @group sql-api
    */
  def col(colName: String): Column


  /**
    * Returns a new Dataframe with an alias set.
    *
    * @group sql-api
    */
  def alias(alias: String): Dataframe

  /**
    * Join with another [[Dataframe]], using the given join expression.
    *
    * {{{
    *   // Scala:
    *   df1.join(df2, df1.col("df1Key") === df2.col("df2Key"), "outer")
    * }}}
    *
    * @param right     Right side of the join.
    * @param joinExprs Join expression.
    * @param joinType  One of: `inner`, `outer`, `left_outer`, `right_outer`, `leftsemi`.
    * @group sql-api
    */
  def join(right: Dataframe, joinExprs: Column, joinType: String): Dataframe

  /**
    * Returns a new [[Dataframe]] by taking the first `n` rows.
    *
    * @group sql-api
    */
  def limit(n: Int): Dataframe

  /**
    * Returns a new Dataframe containing union of rows in this Dataframe and another Dataframe.
    *
    * To do a SQL-style set union (that does deduplication of elements), use this function followed
    * by a [[distinct]].
    *
    * @group sql-api
    */
  def union(other: Dataframe): Dataframe

  /**
    * Returns a new Dataframe containing rows only in both this Dataframe and another Dataframe.
    *
    * Note that, equality checking is performed directly on the encoded representation of the data
    * and thus is not affected by a custom `equals` function.
    *
    * @group sql-api
    */
  def intersect(other: Dataframe): Dataframe

  /**
    * Returns a new Dataframe containing rows in this Dataframe but not in another Dataframe.
    * This is equivalent to `EXCEPT` in SQL.
    *
    * Note that, equality checking is performed directly on the encoded representation of the data
    * and thus is not affected by a custom `equals` function.
    *
    * @group sql-api
    */
  def except(other: Dataframe): Dataframe

  /**
    * Returns a new Dataframe that contains only the unique rows from this Dataframe.
    * This is an alias for [[dropDuplicates(this.columns)]].
    *
    * Note that, equality checking is performed directly on the encoded representation of the data
    * and thus is not affected by a custom `equals` function.
    *
    * @group sql-api
    */
  def distinct(): Dataframe = dropDuplicates(this.columns)

  ////////////////////////////////////////////////////////////////////////////////////
  // GroupBy-related functions
  ////////////////////////////////////////////////////////////////////////////////////

  /**
    * Groups the Dataset using the specified columns, so we can run aggregation on them. See
    * [[GroupedDataframe]] for all the available aggregate functions.
    *
    * {{{
    *   // Compute the average for all numeric columns grouped by department.
    *   ds.groupBy(ds("department")).avg()
    *
    *   // Compute the max age and average salary, grouped by department and gender.
    *   ds.groupBy(ds("department"), ds("gender")).agg(Map(
    *     "salary" -> "avg",
    *     "age" -> "max"
    *   ))
    * }}}
    *
    * @group aggregation
    */
  def groupBy(cols: Column*): GroupedDataframe

  /**
    * Groups the Dataset using the specified columns, so that we can run aggregation on them.
    * See [[GroupedDataframe]] for all the available aggregate functions.
    *
    * This is a variant of groupBy that can only group by existing columns using column names
    * (i.e. cannot construct expressions).
    *
    * @group aggregation
    */
  def groupBy(col1: String, cols: String*): GroupedDataframe = {
    groupBy((col1 +: cols).map(functions.col): _*)
  }

  /**
    * Create a multi-dimensional rollup for the current Dataset using the specified columns,
    * so we can run aggregation on them.
    * See [[GroupedDataframe]] for all the available aggregate functions.
    *
    * {{{
    *   // Compute the average for all numeric columns rolluped by department and group.
    *   ds.rollup(ds("department"), ds("group")).avg()
    *
    *   // Compute the max age and average salary, rolluped by department and gender.
    *   ds.rollup(ds("department"), ds("group")).agg(Map(
    *     "salary" -> "avg",
    *     "age" -> "max"
    *   ))
    * }}}
    *
    * @group aggregation
    */
  def rollup(cols: Column*): GroupedDataframe

  /**
    * Create a multi-dimensional cube for the current Dataset using the specified columns,
    * so we can run aggregation on them.
    * See [[GroupedDataframe]] for all the available aggregate functions.
    *
    * {{{
    *   // Compute the average for all numeric columns cubed by department and group.
    *   ds.cube(ds("department"), ds("group")).avg()
    *
    *   // Compute the max age and average salary, cubed by department and gender.
    *   ds.cube(ds("department"), ds("group")).agg(Map(
    *     "salary" -> "avg",
    *     "age" -> "max"
    *   ))
    * }}}
    *
    * @group aggregation
    */
  def cube(cols: Column*): GroupedDataframe

  /**
    * Aggregates on the entire Dataframe without groups.
    * {{{
    *   // df.agg(...) is a shorthand for df.groupBy().agg(...)
    *   df.agg("age" -> "max", "salary" -> "avg")
    *   df.groupBy().agg("age" -> "max", "salary" -> "avg")
    * }}}
    *
    * @group aggregation
    */
  def agg(aggExpr: (String, String), aggExprs: (String, String)*): Dataframe = {
    groupBy().agg(aggExpr, aggExprs : _*)
  }

  /**
    * Aggregates on the entire Dataframe without groups.
    * {{{
    *   // df.agg(...) is a shorthand for df.groupBy().agg(...)
    *   df.agg("age" -> "max", "salary" -> "avg")
    *   df.groupBy().agg("age" -> "max", "salary" -> "avg")
    * }}}
    *
    * @group aggregation
    */
  def agg(exprs: Map[String, String]): Dataframe = groupBy().agg(exprs)

  /**
    * Aggregates on the entire Dataframe without groups.
    * {{{
    *   // df.agg(...) is a shorthand for df.groupBy().agg(...)
    *   df.agg("age" -> "max", "salary" -> "avg")
    *   df.groupBy().agg("age" -> "max", "salary" -> "avg")
    * }}}
    *
    * @group aggregation
    */
  def agg(expr: Column, exprs: Column*): Dataframe = groupBy().agg(expr, exprs : _*)
}
