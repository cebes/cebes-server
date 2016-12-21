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

package io.cebes.spark.df.support

import io.cebes.df.Dataframe
import io.cebes.df.support.StatFunctions
import io.cebes.spark.df.DataframeFactory
import io.cebes.spark.util.CebesSparkUtil
import org.apache.spark.sql.{DataFrame, DataFrameStatFunctions}

/**
  * Use [[DataframeFactory]] to create new instances of this class
  */
class SparkStatFunctions private[df](dfFactory: DataframeFactory,
                                     sparkStat: DataFrameStatFunctions)
  extends StatFunctions with CebesSparkUtil {

  override def approxQuantile(col: String, probabilities: Array[Double], relativeError: Double): Array[Double] = {
    sparkStat.approxQuantile(col, probabilities, relativeError)
  }

  override def cov(col1: String, col2: String): Double = {
    sparkStat.cov(col1, col2)
  }

  override def corr(col1: String, col2: String, method: String): Double = sparkStat.corr(col1, col2, method)

  override def crosstab(col1: String, col2: String): Dataframe = withSparkDataFrame {
    sparkStat.crosstab(col1, col2)
  }

  override def freqItems(cols: Seq[String], support: Double): Dataframe = withSparkDataFrame {
    sparkStat.freqItems(cols, support)
  }

  override def sampleBy[T](col: String, fractions: Map[T, Double], seed: Long): Dataframe = withSparkDataFrame {
    sparkStat.sampleBy(col, fractions, seed)
  }

  /////////////////////////////////////////////////////////////////////////////
  // Private helpers
  /////////////////////////////////////////////////////////////////////////////

  /**
    * short-hand for returning a SparkDataframe, with proper exception handling
    */
  private def withSparkDataFrame(df: => DataFrame): Dataframe =
    dfFactory.df(safeSparkCall(df))
}
