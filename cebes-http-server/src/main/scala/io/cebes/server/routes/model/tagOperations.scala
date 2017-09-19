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
package io.cebes.server.routes.model

import com.google.inject.Inject
import io.cebes.http.server.result.ResultStorage
import io.cebes.pipeline.ModelService
import io.cebes.pipeline.factory.ModelFactory
import io.cebes.pipeline.json.ModelDef
import io.cebes.pipeline.ml.Model
import io.cebes.server.routes.common._
import io.cebes.store.TagEntry
import io.cebes.tag.Tag

class ModelTagResultTransformer(private val modelFactory: ModelFactory)
  extends TagResultTransformer[Model, ModelDef] {

  override def transformItem(result: Model): Option[ModelDef] = Some(modelFactory.export(result))
}

class ModelTagEntryResultTransformer(private val modelFactory: ModelFactory)
  extends TagEntryResultTransformer[Model, TaggedModelResponse] {

  override def transformTagEntry(tag: Tag, tagEntry: TagEntry, result: Model): TaggedModelResponse = {
    TaggedModelResponse(tag, tagEntry.objectId, tagEntry.createdAt, modelFactory.export(result))
  }
}

class Get @Inject()(modelService: ModelService, modelFactory: ModelFactory,
                    override val resultStorage: ResultStorage)
  extends TagGetAbstract[Model, ModelDef](modelService, new ModelTagResultTransformer(modelFactory))

class TagAdd @Inject()(modelService: ModelService, modelFactory: ModelFactory,
                       override val resultStorage: ResultStorage)
  extends TagAddAbstract[Model, ModelDef](modelService, new ModelTagResultTransformer(modelFactory))

class TagDelete @Inject()(modelService: ModelService, modelFactory: ModelFactory,
                          override val resultStorage: ResultStorage)
  extends TagDeleteAbstract[Model, ModelDef](modelService, new ModelTagResultTransformer(modelFactory))

class Tags @Inject()(modelService: ModelService, modelFactory: ModelFactory,
                     override val resultStorage: ResultStorage)
  extends TagsAbstract[Model, TaggedModelResponse](modelService, new ModelTagEntryResultTransformer(modelFactory))
