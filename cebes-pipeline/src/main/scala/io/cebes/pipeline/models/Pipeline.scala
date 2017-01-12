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

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

case class Pipeline(id: UUID, stages: Map[String, Stage]) extends HasId {

  private val runLocker = new AnyRef()

  def run(outs: Seq[String], feeds: Map[String, PipelineMessage])
         (implicit ec: ExecutionContext): Seq[PipelineMessage] = {
    if (outs.isEmpty) {
      return Nil
    }

    val result = runLocker.synchronized {
      feeds.foreach { case (desc, msg) =>
        val slot = SlotDescriptor(desc)
        stages.get(slot.parent) match {
          case None => throw new IllegalArgumentException(s"Invalid stage name ${slot.parent} in feed $desc")
          case Some(stage) =>
            stage.input(slot.idx, Future(msg))
        }
      }

      Future.sequence(outs.map { desc =>
        val slot = SlotDescriptor(desc)
        stages.get(slot.parent) match {
          case None => throw new IllegalArgumentException(s"Invalid stage name ${slot.parent} in output $desc")
          case Some(stage) => stage.output(slot.idx)
        }
      })
    }
    Await.result(result, Duration.Inf)
  }
}
