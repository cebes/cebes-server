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
 *
 * Created by phvu on 29/12/2016.
 */

package io.cebes.tag

import scala.util.matching.Regex

/**
  * Tag of an object, represented as a name and an optional version
  * The serialized form would be name:version
  *
  * @param name    Name of the object, can have multiple slashes: part1/part2/part3
  *                or with host name and host port: abc.com:500/abc-d/efgh_ijklm
  * @param version version. If user doesn't specify, default will be ``Tag.DEFAULT_VERSION``
  */
case class Tag private(name: String, version: String = Tag.DEFAULT_VERSION) {

  override def toString: String = s"$name:$version"

  /**
    * The full string of host[:port]
    */
  def server: Option[String] = Tag.extract(toString, "server")

  /**
    * Only the host part, if that exists
    */
  def host: Option[String] = Tag.extract(toString, "host")

  def port: Option[Int] = Tag.extract(toString, "port").map(_.toInt)

  /**
    * Everything else follow the host
    */
  def path: Option[String] = Tag.extract(toString, "path")

  /**
    * Return this instance if it already has the "server" part specified,
    * or a new Tag instance with the default host and port explicitly specified
    */
  def withDefaultServer(defaultHost: String, defaultPort: Int): Tag = {
    server match {
      case Some(_) => this
      case None => Tag(s"$defaultHost:$defaultPort/$name", version)
    }
  }
}

object Tag {

  val DEFAULT_VERSION = "default"

  private val tagExprServer: String = """(localhost|[a-z0-9]+([-_][a-z0-9]+)*(\.[a-z0-9]+([-_][a-z0-9]+)*)+)(:([0-9]+))?\/"""
  private val tagExprPath: String = """[a-z0-9]+([-_\.\/][a-z0-9]+)*"""
  private val tagExprVersion: String = """[a-z0-9-_]+"""

  val REGEX_TAG_PATH = new Regex(tagExprPath)
  val REGEX_TAG_VERSION = new Regex(tagExprVersion)

  private val tagExpr = new Regex(
    s"""^(($tagExprServer)?($tagExprPath))(:($tagExprVersion))?$$""",
    "name", "server", "host", "", "", "", "", "port", "path", "", "", "version")

  private def extract(str: String, groupName: String): Option[String] = {
    tagExpr.findFirstMatchIn(str).flatMap(t => Option(t.group(groupName)))
  }

  /**
    * Parse a string into a [[Tag]].
    * If version isn't defined, it will be default as [[Tag.DEFAULT_VERSION]]
    */
  def fromString(str: String): Tag = {
    tagExpr.findFirstMatchIn(str) match {
      case None => throw new IllegalArgumentException(s"Invalid tag expression: $str")
      case Some(m) =>
        Tag(m.group("name"), Option(m.group("version")).getOrElse(Tag.DEFAULT_VERSION))
    }
  }
}
