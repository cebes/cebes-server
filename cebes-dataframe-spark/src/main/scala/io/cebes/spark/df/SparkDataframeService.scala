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
 * Created by phvu on 25/08/16.
 */

package io.cebes.spark.df

import com.google.inject.Inject
import io.cebes.df.{Dataframe, DataframeService}
import io.cebes.spark.config.HasSparkSession

/**
  * Implements [[DataframeService]] on Spark.
  *
  * This class can be instantiated multiple times from the DI framework
  */
class SparkDataframeService @Inject()(hasSparkSession: HasSparkSession) extends DataframeService {

  val sparkSession = hasSparkSession.session

  /**
    * Executes a SQL query, returning the result as a [[Dataframe]].
    *
    * @param sqlText the SQL command to run
    * @return a [[Dataframe]] object
    */
  def sql(sqlText: String): Dataframe = {
    new SparkDataframe(sparkSession.sql(sqlText))
  }
}
