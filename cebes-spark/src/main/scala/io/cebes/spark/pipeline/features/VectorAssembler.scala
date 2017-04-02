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

import com.google.inject.Inject
import io.cebes.df.Dataframe
import io.cebes.pipeline.models.SlotValueMap
import io.cebes.spark.df.SparkDataframeFactory
import org.apache.spark.ml.feature.{VectorAssembler => SparkVectorAssember}

/**
  * A light wrapper of Spark's VectorAssember
  * A feature transformer that merges multiple columns into a vector column.
  */
case class VectorAssembler @Inject()(dfFactory: SparkDataframeFactory)
  extends SparkUnaryTransformer with HasInputCols with HasOutputCol {

  override protected def transform(df: Dataframe, inputs: SlotValueMap, states: SlotValueMap): Dataframe = {
    val assembler = new SparkVectorAssember()
      .setInputCols(inputs(inputCols))
      .setOutputCol(inputs(outputCol))

    val output = assembler.transform(getSparkDataframe(df).sparkDf)
    fromSparkDataframe(dfFactory, output, df.schema, Seq(inputs(outputCol)))
  }
}
