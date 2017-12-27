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
package io.cebes.pipeline

import io.cebes.pipeline.models.Pipeline

import scala.concurrent.Future

/**
  * Store a set of [[io.cebes.pipeline.models.Pipeline]]s, mostly for the purpose of serving,
  * used in [[InferenceService]].
  */
trait InferenceManager {

  case class PipelineInformation(pipeline: Pipeline, slotNamings: Map[String, String])

  /**
    * Optionally get the pipeline of the given serving name
    */
  def getPipeline(servingName: String): Future[PipelineInformation]
}
