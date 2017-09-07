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
import io.cebes.pipeline.ModelService
import io.cebes.pipeline.factory.StageFactory
import io.cebes.pipeline.json.{ModelDef, ModelMessageDef, StageDef}
import io.cebes.pipeline.models.{PipelineMessageSerializer, Stage}
import spray.json.{JsonReader, JsonWriter}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Provide functions for exporting and importing [[Stage]]
  */
class StageExporter @Inject()(private val msgSerializer: PipelineMessageSerializer,
                              private val stageFactory: StageFactory,
                              private val modelExporter: ModelExporter,
                              private val modelService: ModelService) {

  def export(stage: Stage, storageDir: String)
            (implicit jsModelDef: JsonWriter[ModelDef],
             ec: ExecutionContext): Future[StageDef] = {
    val modelStorageDir = getModelSubDir(storageDir)

    stage.toStageDef(msgSerializer).map { stageDef =>
      (stageDef.inputs ++ stageDef.outputs).values.foreach {
        case modelMsg: ModelMessageDef =>
          modelExporter.export(modelService.get(modelMsg.modelId.toString), modelStorageDir,
            getModelDefFile(modelStorageDir, modelMsg.modelId))
        case _ =>
      }
      stageDef
    }
  }

  def imports(stageDef: StageDef, storageDir: String)
             (implicit jsModelDef: JsonReader[ModelDef],
              ec: ExecutionContext): Stage = {

    // need to load all models before actually call create
    val modelStorageDir = getModelSubDir(storageDir)
    (stageDef.inputs ++ stageDef.outputs).values.foreach {
      case modelMsg: ModelMessageDef =>
        modelService.cache(modelExporter.imports(getModelDefFile(modelStorageDir, modelMsg.modelId)))
      case _ =>
    }
    stageFactory.create(stageDef)
  }

  //////////////////////////////////////////////////////////////////////////////////////////
  // Private helpers
  //////////////////////////////////////////////////////////////////////////////////////////

  private def getModelSubDir(storageDir: String) = Paths.get(storageDir, "models").toString

  private def getModelDefFile(modelStorageDir: String, modelId: UUID): String =
    Paths.get(modelStorageDir, modelId.toString).toString
}
