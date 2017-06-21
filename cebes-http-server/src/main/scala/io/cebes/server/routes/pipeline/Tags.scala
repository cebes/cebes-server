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
import io.cebes.pipeline.PipelineService
import io.cebes.server.result.ResultStorage
import io.cebes.server.routes.common.{AsyncSerializableOperation, TaggedPipelineResponse, TagsGetRequest}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Get all tags that match the given pattern.
  */
class Tags @Inject()(pipelineService: PipelineService, override val resultStorage: ResultStorage)
  extends AsyncSerializableOperation[TagsGetRequest, Array[TaggedPipelineResponse]] {

  /**
    * Implement this to do the real work
    */
  override protected def runImpl(requestEntity: TagsGetRequest)
                                (implicit ec: ExecutionContext): Future[Array[TaggedPipelineResponse]] = Future {
    pipelineService.getTags(requestEntity.pattern, requestEntity.maxCount).map {entry =>
      val ppl = pipelineService.get(entry._2.objectId.toString).pipelineDef
      TaggedPipelineResponse(entry._1, entry._2.objectId, entry._2.createdAt, ppl)
    }.toArray
  }
}
