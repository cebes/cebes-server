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

import com.google.inject.Inject
import io.cebes.common.HasId
import io.cebes.pipeline.json.PipelineDef
import io.cebes.pipeline.models.{Pipeline, Stage}

import scala.collection.mutable

class PipelineFactory @Inject()(stageFactory: StageFactory) {

  /**
    * Create the pipeline object from the given definition
    * Note that this will not wire the inputs of stages. That will only be done when
    * [[Pipeline.run()]] is called.
    */
  def create(pipelineDef: PipelineDef): Pipeline = {
    val id = pipelineDef.id.getOrElse(HasId.randomId)

    val stageMap = mutable.Map.empty[String, Stage]
    pipelineDef.stages.map { s =>
      val stage = stageFactory.create(s)
      if (stageMap.contains(stage.getName)) {
        throw new IllegalArgumentException(s"Duplicated stage name: ${stage.getName}")
      }
      stageMap.put(stage.getName, stage)
    }

    Pipeline(id, stageMap.toMap, pipelineDef.copy(id=Some(id)))
  }
}
