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
    * Create a temporary view of this Dataframe,
    * so you can run SQL commands against
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
