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
import io.cebes.pipeline.PipelineService
import io.cebes.pipeline.factory.PipelineFactory
import io.cebes.pipeline.json.{PipelineDef, PipelineMessageDef, PipelineRunDef, StageOutputDef}
import io.cebes.pipeline.models.{Pipeline, SlotDescriptor}
import io.cebes.store.{CachedStore, TagStore}

import scala.concurrent.ExecutionContext

/**
  * Implements [[PipelineService]] on Spark
  */
class SparkPipelineService @Inject()(pipelineFactory: PipelineFactory,
                                     override val cachedStore: CachedStore[Pipeline],
                                     override val tagStore: TagStore[Pipeline]) extends PipelineService {

  /**
    * Create a new pipeline with the given definition.
    * Return the same pipeline definition, with an ID created by the server
    *
    * @param pipelineDef definition of the pipeline
    */
  override def create(pipelineDef: PipelineDef): PipelineDef = fromPipelineDef(pipelineDef).pipelineDef

  /**
    * Run the given pipeline with the given inputs, return the results
    * as a map from stage name to pipeline message.
    *
    * @param runRequest the request.
    *                   See documentation of [[PipelineMessageDef]] for more information.
    * @return A map containing the results of the pipeline.
    *         Will only contain the results of stages requested in the request.
    */
  override def run(runRequest: PipelineRunDef)
                  (implicit ec: ExecutionContext): Map[StageOutputDef, PipelineMessageDef] = {
    val ppl = runRequest.pipeline.id match {
      case Some(id) => get(id.toString)
      case None => fromPipelineDef(runRequest.pipeline)
    }
    val result = ppl.run(runRequest.outputs.map(d => s"${d.stageName}:${d.outputName}"), runRequest.feeds)
    result.map { case (k, v) =>
      val desc = SlotDescriptor(k)
      StageOutputDef(desc.parent, desc.parent) -> v
    }
  }

  /**
    * Utility to create a pipeline object and add it to the store.
    * Return the newly created pipeline
    */
  private def fromPipelineDef(pplDef: PipelineDef): Pipeline = cachedStore.add(pipelineFactory.create(pplDef))
}
