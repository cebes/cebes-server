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
package io.cebes.pipeline

import io.cebes.pipeline.json.{PipelineDef, PipelineMessageDef, PipelineRunDef, StageOutputDef}
import io.cebes.pipeline.models.Pipeline
import io.cebes.tag.TagService

import scala.concurrent.ExecutionContext

trait PipelineService extends TagService[Pipeline] {

  /**
    * Create a new pipeline with the given definition.
    * Return the same pipeline definition, with an ID created by the server
    *
    * @param pipelineDef definition of the pipeline
    */
  def create(pipelineDef: PipelineDef): PipelineDef

  /**
    * Run the given pipeline with the given inputs, return the results
    * as a map from stage name to pipeline message.
    *
    * @param runRequest the request.
    *                   See documentation of [[PipelineMessageDef]] for more information.
    * @return A map containing the results of the pipeline.
    *         Will only contain the results of stages requested in the request.
    */
  def run(runRequest: PipelineRunDef)(implicit ec: ExecutionContext): Map[StageOutputDef, PipelineMessageDef]
}
