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

/**
  * Cebes Dataframe
  */
trait Dataframe extends HasSchema with HasId {

  /**
    * Number of rows
    *
    * @return a long
    */
  def numRows: Long


  /**
    * Sampling functions
    */

  /**
    * Get the first n rows. If the [[Dataframe]] has less than n rows, all rows will be returned.
    * Since the data will be gathered to the memory of a single JVM process,
    * calling this function with big n might cause [[OutOfMemoryError]]
    *
    * @param n number of rows to take
    * @return a [[DataSample]] object containing the data.
    */
  def take(n: Int = 1): DataSample

  /**
    * Randomly sample n rows, return the result as a [[Dataframe]]
    *
    * @param withReplacement Sample with replacement or not.
    * @param fraction        Fraction of rows to generate.
    * @param seed            Seed for sampling.
    * @return a [[Dataframe]] object containing the data.
    */
  def sample(withReplacement: Boolean, fraction: Double, seed: Long): Dataframe

  /**
    * Create a temporary view of this Dataframe,
    * so you can run SQL commands against
    *
    * @param name name of the view
    */
  def createTempView(name: String)

  /**
    * Data exploration
    */


  /**
    * Apply a new schema to this data frame
    *
    * @param newSchema the new Schema
    * @return a new dataframe with the new Schema
    */
  def applySchema(newSchema: Schema): Dataframe
}
