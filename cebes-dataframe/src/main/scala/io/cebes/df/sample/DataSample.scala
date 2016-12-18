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
 * Created by phvu on 05/10/16.
 */

package io.cebes.df.sample

import io.cebes.common.Tabulator
import io.cebes.df.schema.{HasSchema, Schema}

/**
  * A sample of data taken from a [[io.cebes.df.Dataframe]]
  * @param schema data schema
  * @param data real data in column-wise
  */
class DataSample(override val schema: Schema, val data: Seq[Seq[Any]]) extends HasSchema {

  /**
    * Get the data of the given column (case-insensitive)
    */
  def apply(colName: String): Seq[Any] = {
    get(colName).getOrElse(throw new IllegalArgumentException(s"Column name $colName not found"))
  }

  /**
    * Get the data of the given column (case-insensitive)
    */
  def get[T](colName: String): Option[Seq[T]] = {
    schema.zipWithIndex.find(_._1.compareName(colName)).map(item => data(item._2).map(_.asInstanceOf[T]))
  }

  /**
    * Get the rows from the start index to the end index.
    */
  def rows(start: Int, end: Int): Seq[Seq[Any]] =
    data.head.indices.slice(start, end).map(r => data.map(_(r)))

  /**
    * Get all rows of this sample
    */
  def rows: Seq[Seq[Any]] = rows(0, data.head.length)

  /**
    * Tabulated string representation of the sample. For debugging purposes.
    */
  def tabulate(rowsLimit: Int = 10): String = {
    val rowBasedData = schema.fieldNames +: this.rows
    Tabulator.format(rowBasedData)
  }
}
