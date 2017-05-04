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

import io.cebes.common.HasId
import io.cebes.pipeline.json.{PipelineDef, PipelineMessageDef, StageOutputDef}

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/**
  * Don't initialize Pipeline directly. Use [[io.cebes.pipeline.factory.PipelineFactory]] instead
  */
case class Pipeline private[pipeline](id: UUID, stages: Map[String, Stage], pipelineDef: PipelineDef)
  extends HasId {

  private val runLocker = new AnyRef()

  /**
    * Execute the pipeline
    */
  def run(outs: Seq[String], feeds: Map[String, PipelineMessageDef] = Map.empty)
         (implicit ec: ExecutionContext): Map[String, PipelineMessageDef] = {
    if (outs.isEmpty) {
      return Map.empty
    }

    val feedsWithSlot = feeds.map { case (inpDesc, pipelineMsgDef) =>
      val slot = SlotDescriptor(inpDesc)
      stages.get(slot.parent) match {
        case None =>
          throw new IllegalArgumentException(s"Invalid stage name ${slot.parent} in feed $inpDesc")
        case Some(_) => slot -> pipelineMsgDef
      }
    }

    val result = runLocker.synchronized {

      // set everything in feeds which are not StageOutput
      feedsWithSlot.foreach { case (slot, pipelineMsgDef) =>
        pipelineMsgDef match {
          case _: StageOutputDef =>
          case _ =>
            PipelineMessageSerializer.deserialize(pipelineMsgDef, stages(slot.parent), slot.name)
        }
      }

      // now wire the stages, with connections in feeds overwriting connections in the
      // original proto definition
      // Note that wireMap contains the back links: s1 -> s2 meaning s2 provides its output to s1's input
      val wireMap = pipelineDef.stages.flatMap { stageDef =>
        stageDef.inputs.flatMap { case (inpSlot, msgDef) =>
          msgDef match {
            case stageOutputDef: StageOutputDef =>
              Some(SlotDescriptor(stageDef.name, inpSlot) ->
                SlotDescriptor(stageOutputDef.stageName, stageOutputDef.outputName))
            case _ => None
          }
        }
      }.toMap ++ feedsWithSlot.flatMap { case (slot, pipelineMsgDef) =>
        pipelineMsgDef match {
          case stageOutputDef: StageOutputDef =>
            Some(slot -> SlotDescriptor(stageOutputDef.stageName, stageOutputDef.outputName))
          case _ => None
        }
      }

      Pipeline.topoSort(wireMap)

      // wire them all
      wireMap.foreach { case (s1, s2) =>
        val destStage = stages(s1.parent)
        val srcStage = stages(s2.parent)
        destStage.input(destStage.getInput(s1.name), srcStage.output(srcStage.getOutput(s2.name)))
      }

      Future.sequence(outs.map { desc =>
        val slot = SlotDescriptor(desc)
        stages.get(slot.parent) match {
          case None =>
            throw new IllegalArgumentException(s"Invalid stage name ${slot.parent} in output $desc")
          case Some(stage) =>
            val outputSlot = stage.getOutput(slot.name)
            stage.output(outputSlot).getFuture.map { out =>
              desc -> PipelineMessageSerializer.serialize(out, outputSlot)
            }
        }
      }).map(_.toMap)
    }

    Await.result(result, Duration.Inf)
  }
}

object Pipeline {

  private def getIncomingVertices(v: String, edges: Map[SlotDescriptor, SlotDescriptor]): Seq[String] = {
    edges.filter { case (s1, _) =>
      s1.parent == v
    }.map { case (_, s2) =>
      s2.parent
    }.toSeq
  }

  /**
    * Sort the network topologically
    * Note that wireMap contains the back-links: s1 -> s2 means s2 provides its output to s1's input
    */
  private def topoSort(wireMap: Map[SlotDescriptor, SlotDescriptor]): Unit = {
    val sorted = mutable.ListBuffer.empty[String]
    val noInputSet = mutable.Queue.empty[String]
    val hasInputSet = mutable.HashSet.empty[String]
    val vertices = wireMap.flatMap { case (s1, s2) =>
      Seq(s1.parent, s2.parent)
    }.toList.distinct

    vertices.foreach { v =>
      if (getIncomingVertices(v, wireMap).isEmpty) {
        noInputSet.enqueue(v)
      } else {
        hasInputSet += v
      }
    }

    while (noInputSet.nonEmpty) {
      val n = noInputSet.dequeue()
      sorted += n
      val ls = hasInputSet.toList
      ls.foreach { s2 =>
        if (!getIncomingVertices(s2, wireMap).exists { depStageName => hasInputSet.contains(depStageName) }) {
          hasInputSet.remove(s2)
          noInputSet.enqueue(s2)
        }
      }
    }
    require(hasInputSet.isEmpty, s"There is a loop in the pipeline, somewhere around the stages " +
      s"${hasInputSet.mkString(", ")}")
  }
}
