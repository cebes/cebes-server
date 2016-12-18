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

import java.util.UUID

import com.google.inject.Inject
import io.cebes.df.sample.DataSample
import io.cebes.df.{Dataframe, DataframeService, DataframeStore}
import io.cebes.spark.config.HasSparkSession

/**
  * Implements [[DataframeService]] on Spark.
  *
  * This class can be instantiated multiple times from the DI framework
  */
class SparkDataframeService @Inject()(hasSparkSession: HasSparkSession,
                                      dfStore: DataframeStore) extends DataframeService {

  private val sparkSession = hasSparkSession.session


  override def sql(sqlText: String): Dataframe = addToStore {
    new SparkDataframe(sparkSession.sql(sqlText))
  }

  override def count(dfId: UUID): Long = dfStore(dfId).count()

  override def take(dfId: UUID, n: Int): DataSample = {
    dfStore(dfId).take(n)
  }

  override def sample(dfId: UUID, withReplacement: Boolean, fraction: Double, seed: Long): Dataframe = addToStore {
    dfStore(dfId).sample(withReplacement, fraction, seed)
  }

  ////////////////////////////////////////////////////////////////////////////////////
  // Data exploration
  ////////////////////////////////////////////////////////////////////////////////////

  /**
    * Returns a new Dataframe with columns dropped.
    * This is a no-op if schema doesn't contain column name(s).
    */
  override def drop(dfId: UUID, colNames: Seq[String]): Dataframe = addToStore {
    dfStore(dfId).drop(colNames)
  }

  /**
    * Returns a new Dataframe that contains only the unique rows from this Dataframe.
    */
  override def dropDuplicates(dfId: UUID, colNames: Seq[String]): Dataframe = addToStore {
    dfStore(dfId).dropDuplicates(colNames)
  }

  /////////////////////////////////////////////////////////////////////////////
  // Helpers
  /////////////////////////////////////////////////////////////////////////////

  /**
    * Add the Dataframe to the store, and return the dataframe
    */
  private def addToStore(op: => Dataframe): Dataframe = {
    val df = op
    dfStore.add(df)
    df
  }
}
