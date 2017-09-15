/* Copyright 2017 The Cebes Authors. All Rights Reserved.
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
package io.cebes.server.routes.pipeline

import com.google.inject.Inject
import io.cebes.http.server.result.ResultStorage
import io.cebes.pipeline.PipelineService
import io.cebes.pipeline.json.PipelineDef
import io.cebes.pipeline.models.Pipeline
import io.cebes.server.routes.common._
import io.cebes.store.TagEntry
import io.cebes.tag.Tag

class PipelineTagResultTransformer extends TagResultTransformer[Pipeline, PipelineDef] {

  override def transformItem(result: Pipeline): Option[PipelineDef] = Some(result.pipelineDef.copy())
}

class PipelineTagEntryResultTransformer extends TagEntryResultTransformer[Pipeline, TaggedPipelineResponse] {

  override def transformTagEntry(tag: Tag, tagEntry: TagEntry, result: Pipeline): TaggedPipelineResponse = {
    TaggedPipelineResponse(tag, tagEntry.objectId, tagEntry.createdAt, result.pipelineDef.copy())
  }
}

class Get @Inject()(pipelineService: PipelineService, override val resultStorage: ResultStorage)
  extends TagGetAbstract[Pipeline, PipelineDef](pipelineService, new PipelineTagResultTransformer())

class TagAdd @Inject()(pipelineService: PipelineService, override val resultStorage: ResultStorage)
  extends TagAddAbstract[Pipeline, PipelineDef](pipelineService, new PipelineTagResultTransformer())

class TagDelete @Inject()(pipelineService: PipelineService, override val resultStorage: ResultStorage)
  extends TagDeleteAbstract[Pipeline, PipelineDef](pipelineService, new PipelineTagResultTransformer())

class Tags @Inject()(pipelineService: PipelineService, override val resultStorage: ResultStorage)
  extends TagsAbstract[Pipeline, TaggedPipelineResponse](pipelineService, new PipelineTagEntryResultTransformer())
