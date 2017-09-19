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
import io.cebes.df.schema.Schema

////////////////////////////////////////////////////////////////
// Pipeline message
////////////////////////////////////////////////////////////////

trait PipelineMessageDef

case class ValueDef(value: Any) extends PipelineMessageDef

case class StageOutputDef(stageName: String, outputName: String) extends PipelineMessageDef

case class DataframeMessageDef(dfId: UUID) extends PipelineMessageDef

case class SampleMessageDef() extends PipelineMessageDef

case class ModelMessageDef(modelId: UUID) extends PipelineMessageDef

case class ColumnDef(col: Column) extends PipelineMessageDef

/**
  * Definition of a Model, serializable so that it can be sent to clients and persisted to database
  *
  * @param id         ID of the model
  * @param modelClass full class name of the model
  * @param inputs     values of input slots
  * @param metaData   additional meta data
  */
case class ModelDef(id: UUID, modelClass: String, inputs: Map[String, PipelineMessageDef] = Map.empty,
                    metaData: Map[String, String] = Map.empty)

/**
  * Request running a model on the given [[io.cebes.df.Dataframe]]
  *
  * @param model   The model to run
  * @param inputDf the input [[io.cebes.df.Dataframe]]
  */
case class ModelRunDef(model: ModelMessageDef, inputDf: DataframeMessageDef)

/**
  * Definition of a Stage
  *
  * @param name       The name given to this stage. Unique within a single PipelineDef.
  *                   Must satisfy the regex [a-z][a-z0-9_./]*
  * @param stageClass Class name of the stage
  * @param inputs     map of inputs to this stage
  * @param outputs    map from the output slot name to the message
  * @param models     map from a slot name to the model definition [[ModelDef]], if that slot contains a model
  * @param schemas    map from slot names to the [[Schema]] if that slot contains a [[io.cebes.df.Dataframe]]
  */
case class StageDef(name: String, stageClass: String,
                    inputs: Map[String, PipelineMessageDef] = Map.empty,
                    outputs: Map[String, PipelineMessageDef] = Map.empty,
                    models: Map[String, ModelDef] = Map.empty,
                    schemas: Map[String, Schema] = Map.empty)

/**
  * Definition of a Pipeline
  *
  * @param id     The unique ID of this Pipeline.
  *               Normally a UUID generated by the server
  * @param stages list of stages in this pipeline
  */
case class PipelineDef(id: Option[UUID], stages: Array[StageDef])

/**
  * Export of a Pipeline, mainly used by [[io.cebes.pipeline.factory.PipelineFactory]]
  *
  * @param version  version of the exporter
  * @param pipeline the actual pipeline definition
  */
case class PipelineExportDef(version: String, pipeline: PipelineDef)

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
  * @param timeout  The time to wait for the pipeline to finish execution, in seconds.
  *                 Negative values are allowed, indicating Indefinite timeout (wait forever).
  */
case class PipelineRunDef(pipeline: PipelineDef, feeds: Map[String, PipelineMessageDef],
                          outputs: Array[StageOutputDef], timeout: Long = -1)

/**
  * Results of running a pipeline, goes with [[PipelineRunDef]]
  *
  * @param pipelineId the ID of the pipeline being run. If the [[PipelineRunDef]] doesn't contain
  *                   a pipeline ID, it will be generated and returned in this field
  * @param results    The results of all the outputs requested in [[PipelineRunDef]]
  */
case class PipelineRunResultDef(pipelineId: UUID, results: Array[(StageOutputDef, PipelineMessageDef)])
