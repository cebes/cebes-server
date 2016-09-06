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
 * Created by phvu on 05/09/16.
 */

package io.cebes.spark.util

import io.cebes.df.Dataframe
import io.cebes.spark.df.SparkDataframe

object CebesSparkUtil {

  /**
    * Utility function to make sure the [[Dataframe]] passed is a [[SparkDataframe]]
    *
    * @param df a [[Dataframe]] object
    * @return the [[SparkDataframe]] object, or throw an [[IllegalArgumentException]] otherwise
    */
  def getSparkDataframe(df: Dataframe): SparkDataframe = df match {
    case sparkDf: SparkDataframe => sparkDf
    case _ => throw new IllegalArgumentException("Only SparkDataframe can be handled")
  }
}
