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
package io.cebes.tag

import java.util.UUID

import io.cebes.common.HasId
import io.cebes.store.{CachedStore, TagEntry, TagStore}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
  * Generic trait for services that involve managing tagged objects,
  * e.g. [[io.cebes.df.Dataframe]] or Pipeline...
  */
trait TagService[T <: HasId] {

  def cachedStore: CachedStore[T]

  def tagStore: TagStore[T]

  /**
    * Tag an object. Return that object of type [[T]].
    * Raise an exception if the tag already exists.
    */
  def tag(objId: UUID, tag: Tag): T = {
    cachedStore.get(objId) match {
      case None =>
        throw new NoSuchElementException(s"Object ID not found: ${objId.toString}")
      case Some(obj) =>
        Try(tagStore.insert(tag, objId)) match {
          case Success(_) =>
            cachedStore.persist(obj)
            obj
          case Failure(_: IllegalArgumentException) =>
            // throw a user-friendly exception
            throw new IllegalArgumentException(s"Tag ${tag.toString} already exists")
          case Failure(f) => throw f
        }
    }
  }

  /**
    * Remove the given tag.
    * Returns the object of type [[T]] bearing this tag, if it exists.
    * Note that after untagging, the object is still perfectly accessible.
    * It might only be un-accessible if it is kicked out of the cache, and it has no other tags.
    */
  def untag(tag: Tag): T = {
    tagStore.remove(tag) match {
      case Some(entry) =>
        cachedStore.unpersist(entry.objectId) match {
          case Some(obj) => obj
          case None =>
            throw new IllegalStateException(s"Object ID ${entry.objectId.toString} of the given tag " +
              s"(${tag.toString}) doesn't exist in the Store. This is likely a bug, " +
              s"please save the stack trace and notify your administrator")
        }
      case None =>
        throw new NoSuchElementException(s"Tag not found: ${tag.toString}")
    }
  }

  /**
    * Get all the tags and corresponding IDs
    * Optionally submit a regex to filter the tags.
    * Only return `maxCount` entries if there are more than that.
    */
  def getTags(nameRegex: Option[String], maxCount: Int = 100): Iterable[(Tag, TagEntry)] = {
    val elements = tagStore.elements
    val results = mutable.MutableList.empty[(Tag, TagEntry)]
    try {
      nameRegex match {
        case None =>
          while (elements.hasNext && results.lengthCompare(maxCount) < 0) {
            results += elements.next()
          }
        case Some(regexStr) =>
          val regex = regexStr.replace(".", "\\.").replace("*", ".*").replace("?", ".?").r
          while (elements.hasNext && results.lengthCompare(maxCount) < 0) {
            val (tag, entry) = elements.next()
            if (regex.findFirstIn(tag.toString()).isDefined) {
              results += Tuple2(tag, entry)
            }
          }
      }
    } finally {
      elements.close()
    }
    results
  }

  /**
    * Get an object with the given identifier.
    *
    * @param identifier Can be a UUID (for ID) or a tag.
    * @return an object of type [[T]]
    * @throws NoSuchElementException   if the identifier cannot be found.
    * @throws IllegalArgumentException if we fail to parse the identifier as a Tag or ID.
    */
  def get(identifier: String): T = {
    // a bit verbose, so we have meaningful error messages
    val objId = Try(UUID.fromString(identifier)) match {
      case Success(id) => id
      case Failure(_) =>
        Try(Tag.fromString(identifier)) match {
          case Success(tag) => tagStore.get(tag) match {
            case Some(entry) => entry.objectId
            case None =>
              throw new NoSuchElementException(s"Tag not found: $identifier")
          }
          case Failure(_) =>
            throw new IllegalArgumentException(s"Failed to parse Id or Tag: $identifier")
        }
    }

    cachedStore.get(objId) match {
      case Some(obj) => obj
      case None => throw new NoSuchElementException(s"ID not found: ${objId.toString}")
    }
  }
}
