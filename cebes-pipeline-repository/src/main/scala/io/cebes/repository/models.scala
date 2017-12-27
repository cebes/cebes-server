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
package io.cebes.repository

import java.sql.Timestamp

import io.cebes.json.CebesCoreJsonProtocol._
import io.cebes.repository.db.{Repository, RepositoryTag}
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat


case class RepositoryListResponse(repositories: Array[Repository],
                                  pageId: Long,
                                  totalPages: Long)

case class TagResponse(name: String, lastUpdate: Timestamp, repositoryName: Option[String])

case class TagListResponse(repoName: String, tags: Array[TagResponse])

trait CebesRepositoryJsonProtocol {

  implicit val repositoryFormat: RootJsonFormat[Repository] = jsonFormat(Repository,
    "id", "name", "owner", "isPrivate", "pullCount")
  implicit val repositoryTagFormat: RootJsonFormat[RepositoryTag] = jsonFormat(RepositoryTag,
    "id", "repositoryId", "name", "lastUpdate")
  implicit val repositoryListResponseFormat: RootJsonFormat[RepositoryListResponse] =
    jsonFormat3(RepositoryListResponse)
  implicit val tagResponseFormat: RootJsonFormat[TagResponse] = jsonFormat3(TagResponse)
  implicit val tagListResponseFormat: RootJsonFormat[TagListResponse] = jsonFormat2(TagListResponse)
}

object CebesRepositoryJsonProtocol extends CebesRepositoryJsonProtocol
