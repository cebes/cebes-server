/* Copyright 2017 The Cebes Authors. All Rights Reserved.
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
package io.cebes.spark.pipeline

import java.util.UUID
import javax.inject.Inject

import io.cebes.df.{Dataframe, DataframeService}
import io.cebes.pipeline.ModelService
import io.cebes.pipeline.ml.Model
import io.cebes.pipeline.models.PipelineMessageSerializer

/**
  * Implements [[PipelineMessageSerializer]] on Spark
  */
class SparkPipelineMessageSerializer @Inject()(private val dfService: DataframeService,
                                               private val modelService: ModelService)
  extends PipelineMessageSerializer {

  /**
    * Get a [[Dataframe]] object given the ID.
    * This should be implemented by child classes
    */
  override protected def getDataframe(dfId: UUID): Dataframe = dfService.get(dfId.toString)

  /**
    * Get a [[Model]] object given the ID
    * Should be implemented by child classes
    */
  override protected def getModel(modelId: UUID): Model = modelService.get(modelId.toString)
}
