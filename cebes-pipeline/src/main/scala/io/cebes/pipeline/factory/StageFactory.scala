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

import io.cebes.pipeline.models.{PipelineMessageSerializer, Stage}
import io.cebes.pipeline.protos.stage.StageDef

import scala.util.{Success, Try}

class StageFactory(private val stageNamespaces: Seq[String] = Seq("io.cebes.pipeline.models")) {

  /**
    * Construct the stage object, given the proto
    * NOTE: current implementation doesn't consider the "output" in the proto.
    * It will ignore any value specified in the `output` field of the proto message.
    */
  def create(proto: StageDef): Stage = {
    // find the class
    val cls = stageNamespaces.map { ns =>
      Try(Class.forName(s"$ns.${proto.stage}"))
    }.collectFirst {
      case Success(cl) if classOf[Stage].isAssignableFrom(cl) => cl
    } match {
      case Some(cl) => cl
      case None => throw new IllegalArgumentException(s"Stage class not found: ${proto.stage}")
    }

    // construct the stage object, set the right name
    val stage = cls.getConstructors.find { c =>
      c.getParameterCount == 0
    } match {
      case Some(cl) => cl.newInstance().asInstanceOf[Stage].setName(proto.name)
      case None => throw new IllegalArgumentException(s"Failed to initialize stage class ${cls.getName}")
    }

    // set the inputs
    proto.input.foreach { case (inpName, inpMessage) =>
      PipelineMessageSerializer.deserialize(inpMessage, stage, inpName)
    }
    stage
  }
}
