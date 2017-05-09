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
 */
package io.cebes.spark.pipeline.features

import io.cebes.df.Dataframe
import io.cebes.pipeline.models.{HasInputSlots, InputSlot}
import io.cebes.pipeline.stages.UnaryTransformer
import io.cebes.spark.df.SparkDataframeFactory
import io.cebes.spark.util.{CebesSparkUtil, SparkSchemaUtils}
import org.apache.spark.ml.Transformer

trait HasInputCol extends HasInputSlots {

  val inputCol: InputSlot[String] = inputSlot[String]("inputCol",
    "Name of the input column", Some("input"))
}

trait HasInputCols extends HasInputSlots {
  val inputCols: InputSlot[Array[String]] = inputSlot[Array[String]]("inputCols",
    "List of input columns", None)
}

trait HasOutputCol extends HasInputSlots {

  val outputCol: InputSlot[String] = inputSlot[String]("outputCol",
    "Name of the output column", Some("output"))
}

trait SparkUnaryTransformer extends UnaryTransformer with CebesSparkUtil with SparkSchemaUtils {

  /**
    * Helper to return a [[Dataframe]] after a Spark transformation
    * This will help preserve the schema information, taken from `originalSchema`
    */
  final protected def sparkTransform(transformer: Transformer, inputDf: Dataframe,
                                     dfFactory: SparkDataframeFactory,
                                     outputCol: String): Dataframe = {
    val outputSparkDf = transformer.transform(getSparkDataframe(inputDf).sparkDf)
    dfFactory.df(outputSparkDf, getSchema(outputSparkDf, inputDf.schema, outputCol))
  }
}