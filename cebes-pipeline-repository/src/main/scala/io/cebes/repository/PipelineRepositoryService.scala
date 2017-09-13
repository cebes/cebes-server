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

import java.nio.file.Path

import io.cebes.repository.db.Repository

trait PipelineRepositoryService {

  /**
    * Create a new repository of the given name
    * Throw [[IllegalArgumentException]] if it already exists or the name is invalid
    */
  def createRepository(repoName: String, isPrivate: Boolean): Repository

  /**
    * Get the information for the given repository
    * Throw [[NoSuchElementException]] if the repo doesn't exist
    */
  def getRepositoryInfo(repoName: String): Repository

  def listRepositories(pageId: Option[Long]): RepositoryListResponse

  /**
    * List all the tags of the given repo
    * Should throw [[NoSuchElementException]] if the given repo doesn't exist
    */
  def listTags(repoName: String): TagListResponse

  /**
    * Get the meta data of the given tag of the given repo
    * Throw [[NoSuchElementException]] if the given repo/tag doesn't exist
    */
  def getTagInfo(repoName: String, tagName: String): TagResponse

  /**
    * Get the file path to the given repo and tag
    */
  def getPath(repoName: String, tagName: String): Path

  def insertOrUpdateTag(repoName: String, tagName: String): TagResponse
}
