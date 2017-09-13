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
package io.cebes.repository.db

import java.nio.file.{Path, Paths}
import java.sql.Timestamp
import java.util.Calendar

import com.google.inject.Inject
import io.cebes.prop.{Prop, Property}
import io.cebes.repository.db.SquerylEntrypoint._
import io.cebes.repository.{PipelineRepositoryService, RepositoryListResponse, TagListResponse, TagResponse}

/**
  * A simple implementation of [[PipelineRepositoryService]] using SQL backend,
  * without access control
  */
class SimplePipelineRepositoryService @Inject()(@Prop(Property.REPOSITORY_PATH) private val storagePath: String)
  extends PipelineRepositoryService {

  private val pageLength: Int = 100

  override def createRepository(repoName: String, isPrivate: Boolean): Repository = {
    RepositoryDatabase.repositories.insert(Repository(0, repoName, "owner", isPrivate, 0))
  }

  override def getRepositoryInfo(repoName: String): Repository = {
    val repos = from(RepositoryDatabase.repositories)(r => where(r.name === repoName) select r).toArray
    if (repos.isEmpty) {
      throw new NoSuchElementException(s"Repository doesn't exist: $repoName")
    }
    repos.head
  }

  override def listRepositories(pageId: Option[Long]): RepositoryListResponse = {
    val pageIdx = pageId.getOrElse(0L).toInt
    transaction {
      val totalPages = from(RepositoryDatabase.repositories)(_ => compute(count)).toLong
      val repos = from(RepositoryDatabase.repositories)(r => select(r) orderBy r.id.asc)
        .page(pageIdx * pageLength, pageLength).toArray

      RepositoryListResponse(repos, pageIdx, totalPages)
    }
  }

  override def listTags(repoName: String): TagListResponse = {
    val repos = from(RepositoryDatabase.repositories)(r => where(r.name === repoName) select r)
    if (repos.isEmpty) {
      throw new NoSuchElementException(s"Repository doesn't exist: $repoName")
    }

    TagListResponse(repoName,
      from(repos, RepositoryDatabase.repositoryTags)(
        (r, t) => where(r.id === t.repositoryId) select t orderBy t.lastUpdate
      ).map { t =>
        TagResponse(t.name, t.lastUpdate, None)
      }.toArray)
  }

  override def getTagInfo(repoName: String, tagName: String): TagResponse = {
    val arr = from(
      from(RepositoryDatabase.repositories)(r => where(r.name === repoName) select r),
      from(RepositoryDatabase.repositoryTags)(t => where(t.name === tagName) select t))(
      (r, t) => where(r.id === t.repositoryId) select t
    ).toArray
    assert(arr.length <= 1)
    if (arr.isEmpty) {
      throw new NoSuchElementException(s"Repository $repoName:$tagName not found")
    }
    val t = arr.head
    TagResponse(t.name, t.lastUpdate, Some(repoName))
  }

  override def getPath(repoName: String, tagName: String): Path = {
    Paths.get(storagePath, repoName, tagName)
  }

  override def insertOrUpdateTag(repoName: String, tagName: String): TagResponse = {
    val repo = getRepositoryInfo(repoName)
    val updateTime = new Timestamp(Calendar.getInstance().getTimeInMillis)
    RepositoryDatabase.repositoryTags.insertOrUpdate(RepositoryTag(0, repo.id, tagName, updateTime))
    TagResponse(tagName, updateTime, Some(repoName))
  }
}
