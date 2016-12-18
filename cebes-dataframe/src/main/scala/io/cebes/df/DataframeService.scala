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
 * Created by phvu on 23/08/16.
 */

package io.cebes.df

import java.util.UUID

import io.cebes.df.sample.DataSample

trait DataframeService {

  /**
    * Executes a SQL query, returning the result as a [[Dataframe]].
    *
    * @param sqlText the SQL command to run
    * @return a [[Dataframe]] object
    */
  def sql(sqlText: String): Dataframe

  /**
    * Returns the number of rows in the given [[Dataframe]]
    */
  def count(dfId: UUID): Long

  /**
    * Take some rows from the given [[Dataframe]], returns a [[DataSample]]
    */
  def take(dfId: UUID, n: Int): DataSample

  /**
    * Sample the given [[Dataframe]]
    */
  def sample(dfId: UUID, withReplacement: Boolean, fraction: Double, seed: Long): Dataframe

  ////////////////////////////////////////////////////////////////////////////////////
  // Data exploration
  ////////////////////////////////////////////////////////////////////////////////////

  /**
    * Returns a new Dataframe with columns dropped.
    * This is a no-op if schema doesn't contain column name(s).
    */
  def drop(dfId: UUID, colNames: Seq[String]): Dataframe

  /**
    * Returns a new Dataframe that contains only the unique rows from this Dataframe.
    */
  def dropDuplicates(dfId: UUID, colNames: Seq[String]): Dataframe
}
