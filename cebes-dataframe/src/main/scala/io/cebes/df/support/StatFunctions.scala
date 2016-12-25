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

trait StatFunctions {
  /**
    * Calculates the approximate quantiles of a numerical column of a DataFrame.
    *
    * The result of this algorithm has the following deterministic bound:
    * If the Dataframe has N elements and if we request the quantile at probability `p` up to error
    * `err`, then the algorithm will return a sample `x` from the DataFrame so that the *exact* rank
    * of `x` is close to (p * N).
    * More precisely,
    *
    *   floor((p - err) * N) <= rank(x) <= ceil((p + err) * N).
    *
    * This method implements a variation of the Greenwald-Khanna algorithm (with some speed
    * optimizations).
    * The algorithm was first present in [[http://dx.doi.org/10.1145/375663.375670 Space-efficient
    * Online Computation of Quantile Summaries]] by Greenwald and Khanna.
    *
    * @param col the name of the numerical column
    * @param probabilities a list of quantile probabilities
    *   Each number must belong to [0, 1].
    *   For example 0 is the minimum, 0.5 is the median, 1 is the maximum.
    * @param relativeError The relative target precision to achieve (>= 0).
    *   If set to zero, the exact quantiles are computed, which could be very expensive.
    *   Note that values greater than 1 are accepted but give the same result as 1.
    * @return the approximate quantiles at the given probabilities
    */
  def approxQuantile(col: String,
                      probabilities: Array[Double],
                      relativeError: Double): Array[Double]

  /**
    * Calculate the sample covariance of two numerical columns of a DataFrame.
    * @param col1 the name of the first column
    * @param col2 the name of the second column
    * @return the covariance of the two columns.
    */
  def cov(col1: String, col2: String): Double

  /**
    * Calculates the correlation of two columns of a DataFrame. Currently only supports the Pearson
    * Correlation Coefficient.
    *
    * @param col1 the name of the column
    * @param col2 the name of the column to calculate the correlation against
    * @return The Pearson Correlation Coefficient as a Double.
    *
    */
  def corr(col1: String, col2: String, method: String): Double

  /**
    * Calculates the Pearson Correlation Coefficient of two columns of a DataFrame.
    *
    * @param col1 the name of the column
    * @param col2 the name of the column to calculate the correlation against
    * @return The Pearson Correlation Coefficient as a Double.
    */
  def corr(col1: String, col2: String): Double = {
    corr(col1, col2, "pearson")
  }

  /**
    * Computes a pair-wise frequency table of the given columns. Also known as a contingency table.
    * The number of distinct values for each column should be less than 1e4. At most 1e6 non-zero
    * pair frequencies will be returned.
    * The first column of each row will be the distinct values of `col1` and the column names will
    * be the distinct values of `col2`. The name of the first column will be `$col1_$col2`. Counts
    * will be returned as `Long`s. Pairs that have no occurrences will have zero as their counts.
    * Null elements will be replaced by "null", and back ticks will be dropped from elements if they
    * exist.
    *
    * @param col1 The name of the first column. Distinct items will make the first item of
    *             each row.
    * @param col2 The name of the second column. Distinct items will make the column names
    *             of the DataFrame.
    * @return A DataFrame containing for the contingency table.
    *
    * {{{
    *    df
    *    +----+-------+
    *    |key |  value|
    *    +----+-------+
    *    |   1|      1|
    *    |   1|      2|
    *    |   2|      1|
    *    |   2|      1|
    *    |   2|      3|
    *    |   3|      2|
    *    |   3|      3|
    *    +----+-------+
    *
    *    df.stat.crosstab("key", "value")
    *    +---------+---+---+---+
    *    |key_value|  1|  2|  3|
    *    +---------+---+---+---+
    *    |        2|  2|  0|  1|
    *    |        1|  1|  1|  0|
    *    |        3|  0|  1|  1|
    *    +---------+---+---+---+
    * }}}
    */
  def crosstab(col1: String, col2: String): Dataframe

  /**
    * Finding frequent items for columns, possibly with false positives. Using the
    * frequent element count algorithm described in
    * [[http://dx.doi.org/10.1145/762471.762473, proposed by Karp, Schenker, and Papadimitriou]].
    * The `support` should be greater than 1e-4.
    *
    * This function is meant for exploratory data analysis, as we make no guarantee about the
    * backward compatibility of the schema of the resulting [[Dataframe]].
    *
    * @param cols the names of the columns to search frequent items in.
    * @param support The minimum frequency for an item to be considered `frequent`. Should be greater
    *                than 1e-4.
    * @return A Local DataFrame with the Array of frequent items for each column.
    *
    * {{{
    *    // find the items with a frequency greater than 0.4 (observed 40% of the time) for columns
    *    // "a" and "b"
    *    val freqSingles = df.stat.freqItems(Array("a", "b"), 0.4)
    *    freqSingles.show()
    *    +-----------+-------------+
    *    |a_freqItems|  b_freqItems|
    *    +-----------+-------------+
    *    |    [1, 99]|[-1.0, -99.0]|
    *    +-----------+-------------+
    *    // find the pair of items with a frequency greater than 0.1 in columns "a" and "b"
    *    val pairDf = df.select(struct("a", "b").as("a-b"))
    *    val freqPairs = pairDf.stat.freqItems(Array("a-b"), 0.1)
    *    freqPairs.select(explode($"a-b_freqItems").as("freq_ab"))
    *    +----------+
    *    |   freq_ab|
    *    +----------+
    *    |  [1,-1.0]|
    *    |   ...    |
    *    +----------+
    * }}}
    */
  def freqItems(cols: Seq[String], support: Double): Dataframe

  /**
    * Finding frequent items for columns, possibly with false positives. Using the
    * frequent element count algorithm described in
    * [[http://dx.doi.org/10.1145/762471.762473, proposed by Karp, Schenker, and Papadimitriou]].
    * Uses a `default` support of 1%.
    *
    * This function is meant for exploratory data analysis, as we make no guarantee about the
    * backward compatibility of the schema of the resulting [[Dataframe]].
    *
    * @param cols the names of the columns to search frequent items in.
    * @return A Local DataFrame with the Array of frequent items for each column.
    */
  def freqItems(cols: Seq[String]): Dataframe = freqItems(cols, 0.01)

  /**
    * Returns a stratified sample without replacement based on the fraction given on each stratum.
    * @param col column that defines strata
    * @param fractions sampling fraction for each stratum. If a stratum is not specified, we treat
    *                  its fraction as zero.
    * @param seed random seed
    * @tparam T stratum type
    * @return a new [[Dataframe]] that represents the stratified sample
    *
    * {{{
    *    df
    *    +----+-------+
    *    |key |  value|
    *    +----+-------+
    *    |   1|      1|
    *    |   1|      2|
    *    |   2|      1|
    *    |   2|      1|
    *    |   2|      3|
    *    |   3|      2|
    *    |   3|      3|
    *    +----+-------+
    *    val fractions = Map(1 -> 1.0, 3 -> 0.5)
    *    df.stat.sampleBy("key", fractions, 36L)
    *    +---+-----+
    *    |key|value|
    *    +---+-----+
    *    |  1|    1|
    *    |  1|    2|
    *    |  3|    2|
    *    +---+-----+
    * }}}
    */
  def sampleBy[T](col: String, fractions: Map[T, Double], seed: Long): Dataframe
}
