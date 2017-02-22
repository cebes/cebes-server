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
import org.apache.spark.ml.feature.{OneHotEncoder => SparkOneHotEncoder}

/**
  * Created by d066177 on 22/02/2017.
  */
case class OneHotEncoder @Inject()(dfFactory: SparkDataframeFactory) extends SparkUnaryTransformer {

  val dropLast: InputSlot[Boolean] = inputSlot[Boolean]("dropLast",
    "Whether to drop the last category in the encoded vector (default: true)", Some(true))

  override protected def transform(df: Dataframe, inputs: SlotValueMap): Dataframe = {
    val encoder = new SparkOneHotEncoder()
      .setInputCol(inputs(inputCol))
      .setOutputCol(inputs(outputCol))
      .setDropLast(inputs(dropLast))
    val sparkDf = encoder.transform(getSparkDataframe(df).sparkDf)

    fromSparkDf(dfFactory, sparkDf, df.schema, Seq(inputs(outputCol)))
  }
}
