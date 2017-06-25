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

import scala.util.{Success, Try}

class StageFactory @Inject()(private val injector: Injector,
                             @Prop(Property.PIPELINE_STAGE_NAMESPACES) private val stageNamespaces: String) {

  private val stageNamespacesList = stageNamespaces.split(",").map(_.trim)

  /**
    * Construct the stage object, given the proto
    * NOTE: current implementation doesn't consider the "output" in the proto.
    * It will ignore any value specified in the `output` field of the proto message.
    */
  def create(stageDef: StageDef): Stage = {
    // find the class
    val cls = stageNamespacesList.map { ns =>
      Try(Class.forName(s"$ns.${stageDef.stageClass}"))
    }.collect {
      case Success(cl) if classOf[Stage].isAssignableFrom(cl) => cl
    } match {
      case Array() => throw new IllegalArgumentException(s"Stage class not found: ${stageDef.stageClass}")
      case Array(el) => el
      case arr =>
        throw new IllegalArgumentException(s"Multiple stage classes found for ${stageDef.stageClass}: " +
          s"${arr.map(_.getName).mkString(", ")}")
    }

    val stage = injector.getInstance(cls).asInstanceOf[Stage].setName(stageDef.name)
    val serializer = injector.getInstance(classOf[PipelineMessageSerializer])

    // set the inputs
    stage.fromPipelineMessages(stageDef.inputs, serializer)

    // TODO: set the outputs

    stage
  }
}
