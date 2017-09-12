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
 *
 * Created by phvu on 18/12/2016.
 */

package io.cebes.server.routes.pipeline

import com.google.inject.Inject
import io.cebes.http.server.operations.AsyncSerializableOperation
import io.cebes.http.server.result.ResultStorage
import io.cebes.pipeline.PipelineService
import io.cebes.pipeline.json.PipelineDef

import scala.concurrent.{ExecutionContext, Future}

/**
  * Create (i.e. register) a [[PipelineDef]] to the server
  */
class Create @Inject()(pipelineService: PipelineService, override val resultStorage: ResultStorage)
  extends AsyncSerializableOperation[PipelineDef, PipelineDef] {

  override protected def runImpl(requestEntity: PipelineDef)
                                (implicit ec: ExecutionContext): Future[PipelineDef] = Future {
    pipelineService.create(requestEntity)
  }
}
