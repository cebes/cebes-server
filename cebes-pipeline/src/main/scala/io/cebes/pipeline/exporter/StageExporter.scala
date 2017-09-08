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

import java.nio.file.Paths
import java.util.UUID

import com.google.inject.Inject
import io.cebes.pipeline.factory.StageFactory
import io.cebes.pipeline.json.{ModelDef, ModelMessageDef, PipelineMessageDef, StageDef}
import io.cebes.pipeline.ml.Model
import io.cebes.pipeline.models.{PipelineMessageSerializer, Stage}
import spray.json.{JsonReader, JsonWriter}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Provide functions for exporting and importing [[Stage]]
  */
class StageExporter @Inject()(private val msgSerializer: PipelineMessageSerializer,
                              private val stageFactory: StageFactory,
                              private val modelExporter: ModelExporter) {

  /**
    * Export the [[Stage]] into a downloadable format, saved [[Model]] in storageDir if there is any
    */
  def export(stage: Stage, storageDir: String)(implicit jsModelDef: JsonWriter[ModelDef],
                                               ec: ExecutionContext): Future[StageDef] = {
    val modelStorageDir = getModelSubDir(storageDir)

    val serializedInputs = serializePipelineMessages(stage.getInputs(true), modelStorageDir, msgSerializer)
    stage.getOutputs().map { outputs =>
      val serializedOutputs = serializePipelineMessages(outputs, modelStorageDir, msgSerializer)
      StageDef(stage.getName, stage.getClass.getSimpleName, serializedInputs, serializedOutputs)
    }
  }

  /**
    * Import the [[Stage]] that is described by the given [[StageDef]] instance
    */
  def imports(stageDef: StageDef, storageDir: String)(implicit jsModelDef: JsonReader[ModelDef],
                                                      ec: ExecutionContext): Stage = {
    val stage = stageFactory.constructStage(stageDef.stageClass, stageDef.name)

    val modelStorageDir = getModelSubDir(storageDir)
    stage.setInputs(deserializePipelineMessages(stageDef.inputs, modelStorageDir, msgSerializer))
      .setOutputs(deserializePipelineMessages(stageDef.outputs, modelStorageDir, msgSerializer))
  }

  //////////////////////////////////////////////////////////////////////////////////////////
  // Private helpers
  //////////////////////////////////////////////////////////////////////////////////////////

  private def getModelSubDir(storageDir: String) = Paths.get(storageDir, "models").toString

  private def getModelDefFile(modelStorageDir: String, modelId: UUID): String =
    Paths.get(modelStorageDir, s"${modelId.toString}.json").toString

  /**
    * Serialize the pipeline messages, also serialize the [[Model]] instances if there is any
    */
  private def serializePipelineMessages(data: Map[String, Any], modelStorageDir: String,
                                        msgSerializer: PipelineMessageSerializer)
                                       (implicit jsModelDef: JsonWriter[ModelDef]): Map[String, PipelineMessageDef] = {
    data.mapValues {
      case model: Model =>
        // serialize the model
        modelExporter.export(model, modelStorageDir, getModelDefFile(modelStorageDir, model.id))
        msgSerializer.serialize(model)
      case v => msgSerializer.serialize(v)
    }
  }

  /**
    * Deserialize the pipeline messages, with special treatment for [[ModelMessageDef]], where
    * we load the model using the [[modelExporter]]
    */
  private def deserializePipelineMessages(data: Map[String, PipelineMessageDef], modelStorageDir: String,
                                          msgSerializer: PipelineMessageSerializer)
                                         (implicit jsModelDef: JsonReader[ModelDef]): Map[String, Any] = {
    data.mapValues {
      case modelMsgDef: ModelMessageDef =>
        // deserialize the model
        modelExporter.imports(getModelDefFile(modelStorageDir, modelMsgDef.modelId))
      case v => msgSerializer.deserialize(v)
    }
  }
}
