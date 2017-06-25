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

import com.google.inject.Inject
import io.cebes.df.DataframeService
import io.cebes.pipeline.ModelService
import io.cebes.pipeline.factory.ModelFactory
import io.cebes.pipeline.json._
import io.cebes.pipeline.ml.Model
import io.cebes.pipeline.models.PipelineMessageSerializer
import io.cebes.store.{CachedStore, TagStore}

/**
  * Implements [[ModelService]] on Spark
  */
class SparkModelService @Inject()(modelFactory: ModelFactory,
                                  pplMessageSerializer: PipelineMessageSerializer,
                                  dfService: DataframeService,
                                  override val cachedStore: CachedStore[Model],
                                  override val tagStore: TagStore[Model]) extends ModelService {

  override def cache(model: Model): Model = cachedStore.add(model)

  override def run(runRequest: ModelRunDef): DataframeMessageDef = {
    val df = dfService.get(runRequest.inputDf.dfId.toString)
    val dfId = dfService.cache(cachedStore(runRequest.model.modelId).transform(df)).id
    DataframeMessageDef(dfId)
  }

}
