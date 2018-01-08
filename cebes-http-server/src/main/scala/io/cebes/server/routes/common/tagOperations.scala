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

import com.google.inject.Inject
import io.cebes.common.HasId
import io.cebes.http.server.operations.{AsyncOperation, AsyncSerializableOperation}
import io.cebes.http.server.result.ResultStorage
import io.cebes.prop.{Prop, Property}
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

/**
  * Returns all information about the tags that match the given pattern
  */
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

/**
  * Parse the given tag and return each part in the tag, taking all default values into account.
  *
  * This is useful for clients to query information about a tag, without parsing it themselves.
  */
class TagInfo @Inject()(override protected val resultStorage: ResultStorage,
                        @Prop(Property.DEFAULT_REPOSITORY_HOST) private val systemDefaultRepoHost: String,
                        @Prop(Property.DEFAULT_REPOSITORY_PORT) private val systemDefaultRepoPort: Int)
  extends AsyncSerializableOperation[TagInfoRequest, TagInfoResponse] {

  /**
    * Implement this to do the real work
    */
  override protected def runImpl(requestEntity: TagInfoRequest)
                                (implicit ec: ExecutionContext): Future[TagInfoResponse] = Future {
    val tag = requestEntity.tag
    val host = tag.host.orElse(requestEntity.defaultRepoHost).getOrElse(systemDefaultRepoHost)
    val port = tag.port.orElse(requestEntity.defaultRepoPort).getOrElse(systemDefaultRepoPort)
    TagInfoResponse(host, port, tag.path.getOrElse(""), tag.version)
  }
}
