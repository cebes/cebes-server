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
package io.cebes.pipeline.models

import java.util.UUID

import com.google.inject.Inject
import io.cebes.common.HasId
import io.cebes.pipeline.protos.pipeline.PipelineDef
import io.cebes.pipeline.protos.stage.StageDef

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

class PipelineFactory @Inject()(stageFactory: StageFactory) {

  def create(proto: PipelineDef)(implicit ec: ExecutionContext): Pipeline = {
    val id = proto.id.trim match {
      case "" => HasId.randomId
      case s => Try(UUID.fromString(s)) match {
        case Success(existingId) => existingId
        case Failure(f) =>
          throw new IllegalArgumentException(s"Invalid pipeline ID: $s", f)
      }
    }

    // topological sort to make sure there is no loop
    topoSort(proto.stage)

    val stageMap = mutable.Map.empty[String, Stage]
    proto.stage.map { s =>
      val stage = stageFactory.create(s)
      if (stageMap.contains(stage.getName)) {
        throw new IllegalArgumentException(s"Duplicated stage name: ${stage.getName}")
      }
      stageMap.put(stage.getName, stage)
    }
    // wire the inputs
    proto.stage.foreach { stage =>
      stage.input.foreach { case (inpSlot, srcDesc) =>
        val srcSlot = SlotDescriptor(srcDesc)
        stageMap(stage.name).input(inpSlot, stageMap(srcSlot.parent).output(srcSlot.idx))
      }
    }
    Pipeline(id, stageMap.toMap)
  }

  private def topoSort(stages: Seq[StageDef]): Unit = {
    val sorted = mutable.ListBuffer.empty[StageDef]
    val noInputSet = mutable.Queue(stages.filter(_.input.isEmpty): _*)
    val hasInputSet = mutable.HashSet(stages.filter(_.input.nonEmpty): _*)
    while (noInputSet.nonEmpty) {
      val n = noInputSet.dequeue()
      sorted += n
      val ls = hasInputSet.toList
      ls.foreach { s2 =>
        if (!s2.input.exists { case (_, srcDesc) =>
          val srcSlot = SlotDescriptor(srcDesc)
          hasInputSet.exists(_.name == srcSlot.parent)
        }) {
          hasInputSet.remove(s2)
          noInputSet.enqueue(s2)
        }
      }
    }
    require(hasInputSet.isEmpty, "There is a loop in the pipeline")
  }
}
