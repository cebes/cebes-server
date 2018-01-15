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
package io.cebes.server.routes.pipeline

import java.util.UUID

import com.google.inject.Inject
import io.cebes.http.server.operations.AsyncSerializableOperation
import io.cebes.http.server.result.ResultStorage
import io.cebes.pipeline.PipelineService
import io.cebes.prop.{Prop, Property}
import io.cebes.server.routes.common.{TagInfoRequest, TagInfoResponse}
import io.cebes.tag.Tag

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
  * Parse the given tag and return each part in the tag, taking all default values into account.
  *
  * This is useful for clients to query information about a tag, without parsing it themselves.
  */
class TagInfo @Inject()(override protected val resultStorage: ResultStorage,
                        private val pipelineService: PipelineService,
                        @Prop(Property.DEFAULT_REPOSITORY_HOST) private val systemDefaultRepoHost: String,
                        @Prop(Property.DEFAULT_REPOSITORY_PORT) private val systemDefaultRepoPort: Int)
  extends AsyncSerializableOperation[TagInfoRequest, TagInfoResponse] {

  /**
    * Implement this to do the real work
    */
  override protected def runImpl(requestEntity: TagInfoRequest)
                                (implicit ec: ExecutionContext): Future[TagInfoResponse] = Future {
    val tags = Try(UUID.fromString(requestEntity.identifier)) match {
      case Success(id) => pipelineService.find(id)
      case Failure(_) =>
        Try(Tag.fromString(requestEntity.identifier)) match {
          case Success(tag) => Seq(tag)
          case Failure(_) =>
            throw new IllegalArgumentException(s"Invalid pipeline tag or id: ${requestEntity.identifier}")
        }
    }

    if (tags.lengthCompare(1) == 0) {
      val tag = tags.head
      val host = tag.host.orElse(requestEntity.defaultRepoHost).getOrElse(systemDefaultRepoHost)
      val port = tag.port.orElse(requestEntity.defaultRepoPort).getOrElse(systemDefaultRepoPort)
      TagInfoResponse(tag, host, port, tag.path.getOrElse(""), tag.version)
    } else {
      throw new IllegalArgumentException(s"Found 0 or more than 1 tags for ${requestEntity.identifier}")
    }
  }
}