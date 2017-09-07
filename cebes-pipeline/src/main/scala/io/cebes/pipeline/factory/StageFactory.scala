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
import io.cebes.pipeline.json.StageDef
import io.cebes.pipeline.models.{PipelineMessageSerializer, Stage}
import io.cebes.prop.{Prop, Property}

import scala.concurrent.ExecutionContext
import scala.util.{Success, Try}

class StageFactory @Inject()(private val injector: Injector,
                             private val msgSerializer: PipelineMessageSerializer,
                             @Prop(Property.PIPELINE_STAGE_NAMESPACES) private val stageNamespaces: String) {

  private lazy val stageNamespacesList = stageNamespaces.split(",").map(_.trim)

  /**
    * Construct the stage object, given the definition
    */
  def create(stageDef: StageDef)(implicit ec: ExecutionContext): Stage = {
    val stage = constructStage(stageDef.stageClass, stageDef.name)

    // set the inputs and outputs
    stage.setInputs(stageDef.inputs, msgSerializer)
      .setOutputs(stageDef.outputs, msgSerializer)
  }


  /**
    * Construct a new [[Stage]] instance, of the given stageClass and give it the given name
    */
  private def constructStage(stageClass: String, stageName: String): Stage = {
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
}
