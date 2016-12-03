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
 * Created by phvu on 03/12/2016.
 */

package io.cebes.df.support

import io.cebes.df.Dataframe

trait NAFunctions {

  /**
    * Returns a new [[Dataframe]] that drops rows containing any null or NaN values.
    */
  def drop(): Dataframe

  /**
    * Returns a new [[Dataframe]] that drops rows containing null or NaN values.
    *
    * If `how` is "any", then drop rows containing any null or NaN values.
    * If `how` is "all", then drop rows only if every column is null or NaN for that row.
    */
  def drop(how: String): Dataframe

  /**
    * Returns a new [[Dataframe]] that drops rows containing any null or NaN values
    * in the specified columns.
    */
  def drop(cols: Seq[String]): Dataframe

  /**
    * (Scala-specific) Returns a new [[Dataframe]] that drops rows containing null or NaN values
    * in the specified columns.
    *
    * If `how` is "any", then drop rows containing any null or NaN values in the specified columns.
    * If `how` is "all", then drop rows only if every specified column is null or NaN for that row.
    */
  def drop(how: String, cols: Seq[String]): Dataframe = {
    how.toLowerCase match {
      case "any" => drop(cols.size, cols)
      case "all" => drop(1, cols)
      case _ => throw new IllegalArgumentException(s"how ($how) must be 'any' or 'all'")
    }
  }

  /**
    * Returns a new [[Dataframe]] that drops rows containing
    * less than `minNonNulls` non-null and non-NaN values.
    */
  def drop(minNonNulls: Int): Dataframe

  /**
    * Returns a new [[Dataframe]] that drops rows containing
    * less than `minNonNulls` non-null and non-NaN values in the specified columns.
    */
  def drop(minNonNulls: Int, cols: Array[String]): Dataframe

  /**
    * (Scala-specific) Returns a new [[Dataframe]] that drops rows containing less than
    * `minNonNulls` non-null and non-NaN values in the specified columns.
    */
  def drop(minNonNulls: Int, cols: Seq[String]): Dataframe

  /**
    * Returns a new [[Dataframe]] that replaces null or NaN values in numeric columns with `value`.
    */
  def fill(value: Double): Dataframe

  /**
    * Returns a new [[Dataframe]] that replaces null values in string columns with `value`.
    */
  def fill(value: String): Dataframe

  /**
    * Returns a new [[Dataframe]] that replaces null or NaN values in specified
    * numeric columns. If a specified column is not a numeric column, it is ignored.
    */
  def fill(value: Double, cols: Seq[String]): Dataframe

  /**
    * (Scala-specific) Returns a new [[Dataframe]] that replaces null values in
    * specified string columns. If a specified column is not a string column, it is ignored.
    */
  def fill(value: String, cols: Seq[String]): Dataframe

  /**
    * Returns a new [[Dataframe]] that replaces null values.
    *
    * The key of the map is the column name, and the value of the map is the replacement value.
    * The value must be of the following type: `Int`, `Long`, `Float`, `Double`, `String`, `Boolean`.
    * Replacement values are cast to the column data type.
    *
    * For example, the following replaces null values in column "A" with string "unknown", and
    * null values in column "B" with numeric value 1.0.
    * {{{
    *   df.na.fill(Map(
    *     "A" -> "unknown",
    *     "B" -> 1.0
    *   ))
    * }}}
    */
  def fill(valueMap: Map[String, Any]): Dataframe

  /**
    * Replaces values matching keys in `replacement` map.
    * Key and value of `replacement` map must have the same type, and
    * can only be doubles, strings or booleans.
    * If `col` is "*",
    * then the replacement is applied on all string columns , numeric columns or boolean columns.
    *
    * {{{
    *   // Replaces all occurrences of 1.0 with 2.0 in column "height".
    *   df.replace("height", Map(1.0 -> 2.0))
    *
    *   // Replaces all occurrences of "UNKNOWN" with "unnamed" in column "name".
    *   df.replace("name", Map("UNKNOWN" -> "unnamed")
    *
    *   // Replaces all occurrences of "UNKNOWN" with "unnamed" in all string columns.
    *   df.replace("*", Map("UNKNOWN" -> "unnamed")
    * }}}
    *
    * @param col name of the column to apply the value replacement
    * @param replacement value replacement map, as explained above
    *
    * @since 1.3.1
    */
  def replace[T](col: String, replacement: Map[T, T]): Dataframe

  /**
    * Replaces values matching keys in `replacement` map.
    * Key and value of `replacement` map must have the same type, and
    * can only be doubles , strings or booleans.
    *
    * {{{
    *   // Replaces all occurrences of 1.0 with 2.0 in column "height" and "weight".
    *   df.replace("height" :: "weight" :: Nil, Map(1.0 -> 2.0));
    *
    *   // Replaces all occurrences of "UNKNOWN" with "unnamed" in column "firstname" and "lastname".
    *   df.replace("firstname" :: "lastname" :: Nil, Map("UNKNOWN" -> "unnamed");
    * }}}
    *
    * @param cols list of columns to apply the value replacement
    * @param replacement value replacement map, as explained above
    */
  def replace[T](cols: Seq[String], replacement: Map[T, T]): Dataframe
}
