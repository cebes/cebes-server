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
import io.cebes.pipeline.models.{InputSlot, SlotValueMap}
import io.cebes.spark.df.SparkDataframeFactory
import org.apache.spark.ml.feature.{IndexToString => SparkIndexToString}

/**
  * Light wrapper of Spark's IndexToString
  * Maps a column of indices back to a new column of corresponding string values.
  * The index-string mapping is from user-supplied labels.
  */
case class IndexToString @Inject()(dfFactory: SparkDataframeFactory)
  extends SparkUnaryTransformer with HasInputCol with HasOutputCol {

  val labels: InputSlot[Array[String]] = inputSlot[Array[String]]("labels", "Labels used to replace the indices", None)

  /** Implement this function to do the transformation */
  override protected def transform(df: Dataframe, inputs: SlotValueMap, states: SlotValueMap): Dataframe = {
    val reverseIndexer = new SparkIndexToString()
      .setInputCol(inputs(inputCol)).setOutputCol(inputs(outputCol))
      .setLabels(inputs(labels))

    fromSparkDf(dfFactory, reverseIndexer.transform(getSparkDataframe(inputs(inputDf)).sparkDf),
      df.schema, Seq(inputs(outputCol)))
  }
}
