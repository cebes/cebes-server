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

import java.util.concurrent.TimeUnit

import com.google.inject.Inject
import io.cebes.df.{Dataframe, DataframeService}
import io.cebes.pipeline.factory.PipelineFactory
import io.cebes.pipeline.json._
import io.cebes.pipeline.ml.Model
import io.cebes.pipeline.models.{Pipeline, PipelineMessageSerializer, SlotDescriptor}
import io.cebes.pipeline.{ModelService, PipelineService}
import io.cebes.store.{CachedStore, TagStore}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

/**
  * Implements [[PipelineService]] on Spark
  */
class SparkPipelineService @Inject()(override val cachedStore: CachedStore[Pipeline],
                                     override val tagStore: TagStore[Pipeline],
                                     private val pipelineExporter: PipelineFactory,
                                     private val pplMessageSerializer: PipelineMessageSerializer,
                                     private val dfService: DataframeService,
                                     private val modelService: ModelService)
  extends PipelineService {

  /**
    * Create a new pipeline with the given definition.
    * Return the same pipeline definition, with an ID created by the server
    *
    * @param pipelineDef definition of the pipeline
    */
  override def create(pipelineDef: PipelineDef)(implicit ec: ExecutionContext): PipelineDef =
    fromPipelineDef(pipelineDef).pipelineDef

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
                  (implicit ec: ExecutionContext): PipelineRunResultDef = {

    val ppl = runRequest.pipeline.id.map(i => get(i.toString))
      .getOrElse(fromPipelineDef(runRequest.pipeline))

    val outs = runRequest.outputs.map(d => SlotDescriptor(d.stageName, d.outputName)).toSeq
    val feeds = runRequest.feeds.map { case (k, v) =>
      SlotDescriptor(k) -> pplMessageSerializer.deserialize(v)
    }

    val pplId = ppl.id
    val result = ppl.run(outs, feeds).map { result =>
      val results = result.map { case (slot, v) =>

        // cache the results of the pipeline
        v match {
          case df: Dataframe => dfService.cache(df)
          case model: Model => modelService.cache(model)
          case _ =>
        }

        (StageOutputDef(slot.parent, slot.name), pplMessageSerializer.serialize(v))
      }
      PipelineRunResultDef(pplId, results.toArray)
    }

    val waitTime = if (runRequest.timeout <= 0) {
      Duration.Inf
    } else {
      Duration(runRequest.timeout, TimeUnit.SECONDS)
    }
    Await.result(result, waitTime)
  }

  override def cache(ppl: Pipeline): Pipeline = cachedStore.add(ppl)

  /**
    * Utility to create a pipeline object and add it to the store.
    * Return the newly created pipeline
    */
  private def fromPipelineDef(pplDef: PipelineDef)(implicit ec: ExecutionContext): Pipeline =
    cachedStore.add(pipelineExporter.imports(pplDef, None))
}
