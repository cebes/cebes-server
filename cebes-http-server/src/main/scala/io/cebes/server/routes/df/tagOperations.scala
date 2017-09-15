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
package io.cebes.server.routes.df

import com.google.inject.Inject
import io.cebes.df.{Dataframe, DataframeService}
import io.cebes.http.server.result.ResultStorage
import io.cebes.server.routes.common._
import io.cebes.store.TagEntry
import io.cebes.tag.Tag

class DfTagResultTransformer extends TagResultTransformer[Dataframe, DataframeResponse] {

  override def transformItem(result: Dataframe): Option[DataframeResponse] =
    Some(DataframeResponse(result.id, result.schema.copy()))
}

class DfTagEntryResultTransformer extends TagEntryResultTransformer[Dataframe, TaggedDataframeResponse] {

  override def transformTagEntry(tag: Tag, tagEntry: TagEntry, result: Dataframe): TaggedDataframeResponse = {
    TaggedDataframeResponse(tag, tagEntry.objectId, tagEntry.createdAt, result.schema.copy())
  }
}

class Get @Inject()(dfService: DataframeService, override val resultStorage: ResultStorage)
  extends TagGetAbstract[Dataframe, DataframeResponse](dfService, new DfTagResultTransformer())

class TagAdd @Inject()(dfService: DataframeService, override val resultStorage: ResultStorage)
  extends TagAddAbstract[Dataframe, DataframeResponse](dfService, new DfTagResultTransformer())

class TagDelete @Inject()(dfService: DataframeService, override val resultStorage: ResultStorage)
  extends TagDeleteAbstract[Dataframe, DataframeResponse](dfService, new DfTagResultTransformer())

class Tags @Inject()(dfService: DataframeService, override val resultStorage: ResultStorage)
  extends TagsAbstract[Dataframe, TaggedDataframeResponse](dfService, new DfTagEntryResultTransformer())
