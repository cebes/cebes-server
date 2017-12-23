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
package io.cebes.repository.http

import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.server.PathMatcher.{Matched, Matching, Unmatched}
import akka.http.scaladsl.server.PathMatcher1
import io.cebes.tag.Tag

/**
  * Subclass of [[PathMatcher1]] that matches the path of a [[Tag]],
  * used in [[CebesRepositoryServer]]
  */
private[http] object RepositoryName extends PathMatcher1[String] {
  def apply(path: Path): Matching[Tuple1[String]] = {
    val sPath = path.toString()
    Tag.REGEX_TAG_PATH.findPrefixMatchOf(sPath) match {
      case Some(m) if m.end == sPath.length => Matched(Path.Empty, Tuple1(sPath))
      case _ => Unmatched
    }
  }
}
