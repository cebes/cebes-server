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
package io.cebes.spark.pipeline.ml.traits

import io.cebes.df.Dataframe
import io.cebes.pipeline.ml.Model
import io.cebes.pipeline.models.SlotValueMap
import io.cebes.spark.df.SparkDataframeFactory
import io.cebes.spark.util.CebesSparkUtil
import org.apache.spark.ml.Transformer

/**
  * Generic trait for Spark Machine Learning model
  */
trait SparkModel extends Model with CebesSparkUtil {

  val sparkTransformer: Transformer

  val dfFactory: SparkDataframeFactory

  /** Implement this to do the real transformation */
  override def transformImpl(data: Dataframe, params: SlotValueMap): Dataframe = {
    val sparkDf = sparkTransformer.transform(getSparkDataframe(data).sparkDf)

    //TODO: copy schema information from "data" to the resulting DF
    dfFactory.df(sparkDf)
  }
}
