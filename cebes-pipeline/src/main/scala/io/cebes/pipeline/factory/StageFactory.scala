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
package io.cebes.pipeline.factory

import com.google.inject.{Inject, Injector}
import io.cebes.df.Dataframe
import io.cebes.df.schema.Schema
import io.cebes.pipeline.json.{ModelDef, ModelMessageDef, PipelineMessageDef, StageDef}
import io.cebes.pipeline.ml.Model
import io.cebes.pipeline.models.{PipelineMessageSerializer, Stage}
import io.cebes.prop.{Prop, Property}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try}

/**
  * Provide functions for exporting and importing [[Stage]]
  */
class StageFactory @Inject()(private val msgSerializer: PipelineMessageSerializer,
                             private val modelFactory: ModelFactory,
                             private val injector: Injector,
                             @Prop(Property.PIPELINE_STAGE_NAMESPACES) private val stageNamespaces: String) {

  /**
    * Export the [[Stage]] into a downloadable format, saved [[Model]] in modelStorageDir if there is any
    */
  def export(stage: Stage, options: PipelineExportOptions)(implicit ec: ExecutionContext): Future[StageDef] = {
    val modelDefs = mutable.Map.empty[String, ModelDef]
    val schemas = mutable.Map.empty[String, Schema]

    val serializedInputs = serializePipelineMessages(stage.getInputs(), msgSerializer, options, modelDefs, schemas)

    stage.getOutputs().map { outputs =>
      val serializedOutputs = serializePipelineMessages(outputs, msgSerializer, options, modelDefs, schemas)
      StageDef(stage.getName, stage.getClass.getSimpleName, serializedInputs, serializedOutputs,
        models = modelDefs.toMap, schemas = schemas.toMap, newInputs = stage.getNewInputFlag)
    }
  }

  /**
    * Import the [[Stage]] that is described by the given [[StageDef]] instance
    */
  def imports(stageDef: StageDef, modelStorageDir: Option[String])(implicit ec: ExecutionContext): Stage = {
    val stage = constructStage(stageDef.stageClass, stageDef.name)

    stage.setInputs(deserializePipelineMessages(stageDef.inputs, modelStorageDir, msgSerializer, stageDef.models))
      .setOutputs(deserializePipelineMessages(stageDef.outputs, modelStorageDir, msgSerializer, stageDef.models))
      .setNewInputFlag(stageDef.newInputs)
  }

  //////////////////////////////////////////////////////////////////////////////////////////
  // Private helpers
  //////////////////////////////////////////////////////////////////////////////////////////

  private lazy val stageNamespacesList = stageNamespaces.split(",").map(_.trim)

  /**
    * Construct a new [[Stage]] instance, of the given stageClass and give it the given name
    */
  def constructStage(stageClass: String, stageName: String): Stage = {
    // find the class
    val cls = stageNamespacesList.map { ns =>
      Try(Class.forName(s"$ns.$stageClass"))
    }.collect {
      case Success(cl) if classOf[Stage].isAssignableFrom(cl) => cl
    } match {
      case Array() => throw new IllegalArgumentException(s"Stage class not found: $stageClass")
      case Array(el) => el
      case arr =>
        throw new IllegalArgumentException(s"Multiple stage classes found for $stageClass: " +
          s"${arr.map(_.getName).mkString(", ")}")
    }

    injector.getInstance(cls).asInstanceOf[Stage].setName(stageName)
  }

  /**
    * Serialize the pipeline messages, also serialize the [[Model]] instances if there is any
    */
  private def serializePipelineMessages(data: Map[String, Any],
                                        msgSerializer: PipelineMessageSerializer,
                                        options: PipelineExportOptions,
                                        models: mutable.Map[String, ModelDef],
                                        schemas: mutable.Map[String, Schema]): Map[String, PipelineMessageDef] = {
    data.flatMap { case (slotName, value) =>
      value match {
        case model: Model =>
          if (options.includeModels) {
            models.put(slotName, modelFactory.export(model, options.modelStorageDir))
          }
          Some(slotName -> msgSerializer.serialize(value))
        case df: Dataframe =>
          if (options.includeSchemas) {
            schemas.put(slotName, df.schema)
          }
          if (options.includeDataframes) {
            Some(slotName -> msgSerializer.serialize(value))
          } else {
            None
          }
        case _ =>
          Some(slotName -> msgSerializer.serialize(value))
      }
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
            modelFactory.imports(modelDefs(slotName), modelStorageDir)
          } else {
            msgSerializer.deserialize(msg)
          }
        case _ => msgSerializer.deserialize(msg)
      }
      slotName -> value
    }
  }
}
