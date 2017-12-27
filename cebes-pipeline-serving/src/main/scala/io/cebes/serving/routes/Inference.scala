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
package io.cebes.serving.routes

import com.google.inject.Inject
import io.cebes.http.server.operations.AsyncSerializableOperation
import io.cebes.http.server.result.ResultStorage
import io.cebes.pipeline.InferenceService
import io.cebes.pipeline.json.{InferenceRequest, InferenceResponse}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Run inference asynchronously
  */
class Inference @Inject()(private val inferenceService: InferenceService,
                          override val resultStorage: ResultStorage)
  extends AsyncSerializableOperation[InferenceRequest, InferenceResponse] {

  override protected def runImpl(requestEntity: InferenceRequest)
                                (implicit ec: ExecutionContext): Future[InferenceResponse] = {
    inferenceService.inference(requestEntity)
  }
}
