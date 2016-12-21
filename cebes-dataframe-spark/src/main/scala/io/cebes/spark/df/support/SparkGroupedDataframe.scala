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

package io.cebes.spark.df.support

import com.google.inject.Inject
import com.google.inject.assistedinject.Assisted
import io.cebes.df.support.GroupedDataframe
import io.cebes.df.{Column, Dataframe}
import io.cebes.spark.df.DataframeFactory
import io.cebes.spark.df.expressions.SparkExpressionParser
import io.cebes.spark.util.CebesSparkUtil
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset}

class SparkGroupedDataframe @Inject() private[df](private val dfFactory: DataframeFactory,
                                                  private val parser: SparkExpressionParser,
                                                  @Assisted sparkGroupedDataset: RelationalGroupedDataset)
  extends GroupedDataframe with CebesSparkUtil {

  override def agg(exprs: Map[String, String]): Dataframe = withSparkDataFrame {
    sparkGroupedDataset.agg(exprs)
  }

  override def agg(expr: Column, exprs: Column*): Dataframe = {
    val cols = parser.toSpark(expr +: exprs)
    withSparkDataFrame(sparkGroupedDataset.agg(cols.head, cols.tail: _*))
  }

  override def count(): Dataframe = withSparkDataFrame {
    sparkGroupedDataset.count()
  }

  override def max(colNames: String*): Dataframe = withSparkDataFrame {
    sparkGroupedDataset.max(colNames: _*)
  }

  override def avg(colNames: String*): Dataframe = withSparkDataFrame {
    sparkGroupedDataset.avg(colNames: _*)
  }

  override def min(colNames: String*): Dataframe = withSparkDataFrame {
    sparkGroupedDataset.min(colNames: _*)
  }

  override def sum(colNames: String*): Dataframe = withSparkDataFrame {
    sparkGroupedDataset.sum(colNames: _*)
  }

  override def pivot(pivotColumn: String): GroupedDataframe = {
    dfFactory.groupedDf(safeSparkCall(sparkGroupedDataset.pivot(pivotColumn)))
  }

  override def pivot(pivotColumn: String, values: Seq[Any]): GroupedDataframe = {
    dfFactory.groupedDf(safeSparkCall(sparkGroupedDataset.pivot(pivotColumn, values)))
  }

  /////////////////////////////////////////////////////////////////////////////
  // Private helpers
  /////////////////////////////////////////////////////////////////////////////

  /**
    * short-hand for returning a SparkDataframe, with proper exception handling
    */
  private def withSparkDataFrame(df: => DataFrame): Dataframe = dfFactory.df(safeSparkCall(df))

}
