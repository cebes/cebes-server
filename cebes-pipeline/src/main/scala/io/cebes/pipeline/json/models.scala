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

package io.cebes.pipeline.json

import java.util.UUID

import io.cebes.df.Column

////////////////////////////////////////////////////////////////
// Pipeline message
////////////////////////////////////////////////////////////////

trait PipelineMessageDef

case class ValueDef(value: Any) extends PipelineMessageDef

case class StageOutputDef(stageName: String, outputName: String) extends PipelineMessageDef

case class DataframeMessageDef() extends PipelineMessageDef

case class SampleMessageDef() extends PipelineMessageDef

case class ModelMessageDef() extends PipelineMessageDef

case class ColumnDef(col: Column) extends PipelineMessageDef

/**
  * Definition of a Stage
  *
  * @param name       The name given to this stage. Unique within a single PipelineDef.
  *                   Must satisfy the regex [a-z][a-z0-9_./]*
  * @param stageClass Class name of the stage
  * @param inputs     map of inputs to this stage. Each entry is input_slot -> src_stage[:src_slot]
  * @param outputs    map from the output slot name to the message
  */
case class StageDef(name: String, stageClass: String, inputs: Map[String, PipelineMessageDef] = Map.empty,
                    outputs: Map[String, PipelineMessageDef] = Map.empty)

/**
  * Definition of a Pipeline
  *
  * @param id     The unique ID of this Pipeline.
  *               Normally a UUID generated by the server
  * @param stages list of stages in this pipeline
  */
case class PipelineDef(id: Option[UUID], stages: Array[StageDef])

/**
  * request running a pipeline given the input in feeds, and requesting output at slots specified in outputs
  *
  * @param pipeline the pipeline to be run, can be a full pipeline definition
  *                 or just the ID of the pipeline,
  *                 in which case it will be used to look up the pipeline on the server
  * @param feeds    the input map for some of the input slot.
  *                 Each key is "stage:input_slot" with "stage" being a string name and
  *                 "input_slot" indicating which input slot to use from "stage".
  *                 If "input_slot" is default, then the ":default" suffix can be omitted.
  * @param outputs  which output slot to be collected
  */
case class PipelineRunDef(pipeline: PipelineDef, feeds: Map[String, PipelineMessageDef],
                          outputs: Array[StageOutputDef])
