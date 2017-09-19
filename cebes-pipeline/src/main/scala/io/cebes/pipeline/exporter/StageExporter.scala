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
package io.cebes.pipeline.exporter

import com.google.inject.Inject
import io.cebes.df.Dataframe
import io.cebes.df.schema.Schema
import io.cebes.pipeline.factory.{ModelFactory, StageFactory}
import io.cebes.pipeline.json.{ModelDef, ModelMessageDef, PipelineMessageDef, StageDef}
import io.cebes.pipeline.ml.Model
import io.cebes.pipeline.models.{PipelineMessageSerializer, Stage}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/**
  * Provide functions for exporting and importing [[Stage]]
  */
class StageExporter @Inject()(private val msgSerializer: PipelineMessageSerializer,
                              private val stageFactory: StageFactory,
                              private val modelFactory: ModelFactory) {

  /**
    * Export the [[Stage]] into a downloadable format, saved [[Model]] in modelStorageDir if there is any
    */
  def export(stage: Stage, modelStorageDir: Option[String])(implicit ec: ExecutionContext): Future[StageDef] = {
    val modelDefs = mutable.Map.empty[String, ModelDef]
    val schemas = mutable.Map.empty[String, Schema]

    val serializedInputs = serializePipelineMessages(stage.getInputs(true),
      modelStorageDir, msgSerializer, modelDefs, schemas)

    stage.getOutputs().map { outputs =>
      val serializedOutputs = serializePipelineMessages(outputs, modelStorageDir, msgSerializer, modelDefs, schemas)
      StageDef(stage.getName, stage.getClass.getSimpleName, serializedInputs, serializedOutputs,
        models = modelDefs.toMap, schemas = schemas.toMap)
    }
  }

  /**
    * Import the [[Stage]] that is described by the given [[StageDef]] instance
    */
  def imports(stageDef: StageDef, modelStorageDir: Option[String])(implicit ec: ExecutionContext): Stage = {
    val stage = stageFactory.constructStage(stageDef.stageClass, stageDef.name)

    stage.setInputs(deserializePipelineMessages(stageDef.inputs, modelStorageDir, msgSerializer, stageDef.models))
      .setOutputs(deserializePipelineMessages(stageDef.outputs, modelStorageDir, msgSerializer, stageDef.models))
  }

  //////////////////////////////////////////////////////////////////////////////////////////
  // Private helpers
  //////////////////////////////////////////////////////////////////////////////////////////

  /**
    * Serialize the pipeline messages, also serialize the [[Model]] instances if there is any
    */
  private def serializePipelineMessages(data: Map[String, Any], modelStorageDir: Option[String],
                                        msgSerializer: PipelineMessageSerializer,
                                        models: mutable.Map[String, ModelDef],
                                        schemas: mutable.Map[String, Schema]): Map[String, PipelineMessageDef] = {
    data.map { case (slotName, value) =>
      value match {
        case model: Model =>
          models.put(slotName, modelFactory.save(model, modelStorageDir))
        case df: Dataframe =>
          schemas.put(slotName, df.schema)
        case _ =>
      }
      slotName -> msgSerializer.serialize(value)
    }
  }

  /**
    * Deserialize the pipeline messages, with special treatment for [[ModelMessageDef]], where
    * we load the model using the [[modelFactory]]
    */
  private def deserializePipelineMessages(data: Map[String, PipelineMessageDef], modelStorageDir: Option[String],
                                          msgSerializer: PipelineMessageSerializer,
                                          modelDefs: Map[String, ModelDef]): Map[String, Any] = {
    data.map { case (slotName, msg) =>
      val value = msg match {
        case _: ModelMessageDef =>
          if (modelDefs.contains(slotName)) {
            modelFactory.create(modelDefs(slotName), modelStorageDir)
          } else {
            msgSerializer.deserialize(msg)
          }
        case _ => msgSerializer.deserialize(msg)
      }
      slotName -> value
    }
  }
}
