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
package io.cebes.server.routes.common

import io.cebes.common.HasId
import io.cebes.http.server.operations.{AsyncOperation, AsyncSerializableOperation}
import io.cebes.store.TagEntry
import io.cebes.tag.{Tag, TagService}

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

trait TagResultTransformer[T, R] {

  /**
    * Transform the tagged item (Dataframe/Pipeline...) to corresponding DataframeResponse or PipelineDef
    * Used in [[TagGetAbstract]], [[TagAddAbstract]] and [[TagDeleteAbstract]]
    */
  def transformItem(result: T): Option[R]
}

trait TagEntryResultTransformer[T, R] {
  /**
    * Transform a TagEntry into the customized result,
    * typically contains the tag, the object ID and some additional information
    * Used in [[TagsAbstract]]
    */
  def transformTagEntry(tag: Tag, tagEntry: TagEntry, result: T): R
}

/**
  * Get the object identified by either the ID or tag
  */
abstract class TagGetAbstract[T <: HasId, R](protected val tagService: TagService[T],
                                             protected val resultTransformer: TagResultTransformer[T, R])
  extends AsyncOperation[String, T, R] {

  override protected def runImpl(requestEntity: String)(implicit ec: ExecutionContext): Future[T] = Future {
    tagService.get(requestEntity.stripPrefix("\"").stripSuffix("\""))
  }

  override protected def transformResult(requestEntity: String, result: T): Option[R] =
    resultTransformer.transformItem(result)
}

/**
  * Tag the given object
  */
abstract class TagAddAbstract[T <: HasId, R](protected val tagService: TagService[T],
                                             protected val resultTransformer: TagResultTransformer[T, R])
  extends AsyncOperation[TagAddRequest, T, R] {

  override protected def runImpl(requestEntity: TagAddRequest)
                                (implicit ec: ExecutionContext): Future[T] = Future {
    tagService.tag(requestEntity.objectId, requestEntity.tag)
  }

  override protected def transformResult(requestEntity: TagAddRequest, result: T): Option[R] =
    resultTransformer.transformItem(result)
}

/**
  * Delete the given tag
  */
abstract class TagDeleteAbstract[T <: HasId, R](protected val tagService: TagService[T],
                                                protected val resultTransformer: TagResultTransformer[T, R])
  extends AsyncOperation[TagDeleteRequest, T, R] {

  override protected def runImpl(requestEntity: TagDeleteRequest)
                                (implicit ec: ExecutionContext): Future[T] = Future {
    tagService.untag(requestEntity.tag)
  }

  override protected def transformResult(requestEntity: TagDeleteRequest, result: T): Option[R] =
    resultTransformer.transformItem(result)
}

abstract class TagsAbstract[T <: HasId, R](protected val tagService: TagService[T],
                                           protected val tagEntryTransformer: TagEntryResultTransformer[T, R])
                                          (implicit classTag: ClassTag[R])
  extends AsyncSerializableOperation[TagsGetRequest, Array[R]] {

  /**
    * Implement this to do the real work
    */
  override protected def runImpl(requestEntity: TagsGetRequest)
                                (implicit ec: ExecutionContext): Future[Array[R]] = Future {
    tagService.getTags(requestEntity.pattern, requestEntity.maxCount).map { entry =>
      val item = tagService.get(entry._2.objectId.toString)
      tagEntryTransformer.transformTagEntry(entry._1, entry._2, item)
    }.toArray
  }
}