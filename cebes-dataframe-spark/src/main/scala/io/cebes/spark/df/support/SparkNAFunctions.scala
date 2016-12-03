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
import io.cebes.df.support.NAFunctions
import io.cebes.spark.util.CebesSparkUtil
import org.apache.spark.sql.DataFrameNaFunctions

class SparkNAFunctions private[df](sparkNA: DataFrameNaFunctions) extends NAFunctions with CebesSparkUtil {

  override def drop(how: String): Dataframe = withSparkDataFrame(sparkNA.drop(how))

  override def drop(minNonNulls: Int): Dataframe = withSparkDataFrame(sparkNA.drop(minNonNulls))

  override def drop(minNonNulls: Int, cols: Seq[String]): Dataframe = withSparkDataFrame {
    sparkNA.drop(minNonNulls, cols)
  }

  override def fill(value: Double): Dataframe = withSparkDataFrame(sparkNA.fill(value))

  override def fill(value: String): Dataframe = withSparkDataFrame(sparkNA.fill(value))

  override def fill(value: Double, cols: Seq[String]): Dataframe = withSparkDataFrame(sparkNA.fill(value, cols))

  override def fill(value: String, cols: Seq[String]): Dataframe = withSparkDataFrame(sparkNA.fill(value, cols))

  override def fill(valueMap: Map[String, Any]): Dataframe = withSparkDataFrame(sparkNA.fill(valueMap))


  override def replace[T](col: String, replacement: Map[T, T]): Dataframe = withSparkDataFrame {
    sparkNA.replace(col, replacement)
  }

  override def replace[T](cols: Seq[String], replacement: Map[T, T]): Dataframe = withSparkDataFrame {
    sparkNA.replace(cols, replacement)
  }
}
