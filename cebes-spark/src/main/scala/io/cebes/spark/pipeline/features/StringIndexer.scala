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
import io.cebes.pipeline.models.{OutputSlot, SlotValueMap}
import io.cebes.spark.df.SparkDataframeFactory
import org.apache.spark.ml.feature.{StringIndexerModel, StringIndexer => SparkStringIndexer}

/**
  * Light wrapper of Spark's StringIndexer
  * A label indexer that maps a string column of labels to an ML column of label indices.
  * If the input column is numeric, we cast it to string and index the string values.
  * The indices are in [0, numLabels), ordered by label frequencies.
  * So the most frequent label gets index 0
  */
case class StringIndexer @Inject()(dfFactory: SparkDataframeFactory)
  extends SparkUnaryTransformer with HasInputCol with HasOutputCol {

  val model: OutputSlot[Array[String]] = outputSlot[Array[String]]("model",
    "The model estimated by the string indexer", None, stateful = true)

  override protected def computeStatefulOutput(inputs: SlotValueMap, stateSlot: OutputSlot[Any]): Any = {
    require(stateSlot == model, s"Only 'model' is supported, got ${stateSlot.name}: ${stateSlot.doc}")
    val indexer = new SparkStringIndexer()
      .setInputCol(inputs(inputCol))
      .setOutputCol(inputs(outputCol))
    val indexerModel = safeSparkCall(indexer.fit(getSparkDataframe(inputs(inputDf)).sparkDf))
    indexerModel.labels
  }

  /** Implement this function to do the transformation */
  override protected def transform(df: Dataframe, inputs: SlotValueMap, states: SlotValueMap): Dataframe = {
    val indexerModel = new StringIndexerModel(states(model))
      .setInputCol(inputs(inputCol)).setOutputCol(inputs(outputCol))
    fromSparkDf(dfFactory, indexerModel.transform(getSparkDataframe(inputs(inputDf)).sparkDf),
      df.schema, Seq(inputs(outputCol)))
  }
}
