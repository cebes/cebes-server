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
import io.cebes.pipeline.protos.pipeline.PipelineDef

import scala.util.{Failure, Success, Try}

case class Pipeline(id: UUID, stages: Map[String, Stage]) extends HasId {


  def run(outs: Seq[String], feeds: Map[String, PipelineMessage]): Seq[PipelineMessage] = {
    if (outs.isEmpty) {
      return Nil
    }

    Nil
  }
}

object Pipeline {

  def fromProto(proto: PipelineDef): Pipeline = {
    val id = proto.id match {
      case "" => UUID.randomUUID()
      case s => Try(UUID.fromString(s)) match {
        case Success(d) => d
        case Failure(f) => throw new IllegalArgumentException(s"Invalid ID: $s", f)
      }
    }
    new Pipeline(id, Map.empty)
  }
}
