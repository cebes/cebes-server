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
 * Created by phvu on 19/12/2016.
 */

package io.cebes.spark.df

import java.util.UUID

import com.google.inject.{Inject, Injector, Singleton}
import io.cebes.common.HasId
import io.cebes.df.Dataframe
import io.cebes.df.schema.Schema
import io.cebes.spark.df.expressions.SparkExpressionParser
import io.cebes.spark.util.SparkSchemaUtils
import org.apache.spark.sql.DataFrame

/**
  * Factory for SparkDataframe, to be used in DI framework
  */
@Singleton class SparkDataframeFactory @Inject()(private val injector: Injector) {

  /**
    * Returns a new instance of [[Dataframe]]
    */
  def df(sparkDf: DataFrame, schema: Schema, id: UUID): Dataframe = {
    val sparkDf2 = if (sparkDf.columns.length > 1 &&
      sparkDf.columns.length == schema.length &&
      sparkDf.columns.forall(s => schema.exists(_.compareName(s)) &&
        !sparkDf.columns.zip(schema).forall(t => t._2.compareName(t._1)))) {
      // re-arrange the columns in dataframe.
      // This is needed after, e.g. a JSON serialization
      val colNames = schema.fieldNames
      sparkDf.select(colNames.head, colNames.tail: _*)
    } else {
      sparkDf
    }
    new SparkDataframe(this, injector.getInstance(classOf[SparkExpressionParser]), sparkDf2, schema, id)
  }

  /**
    * Returns a new instance of [[Dataframe]], with a random ID
    */
  def df(sparkDf: DataFrame, schema: Schema): Dataframe =
    df(sparkDf, schema, HasId.randomId)

  /**
    * Returns a new instance of [[Dataframe]], with a random ID and an automatically-inferred Schema
    */
  def df(sparkDf: DataFrame): Dataframe =
    df(sparkDf, SparkSchemaUtils.getSchema(sparkDf), HasId.randomId)
}
