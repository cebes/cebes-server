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

package io.cebes.common

import scala.util.matching.Regex

/**
  * Tag of an object, represented as a name and an optional version
  * The serialized form would be name:version
  *
  * @param name Name of the object, can have multiple slashes: part1/part2/part3
  * @param version version
  */
case class Tag private(name: String, version: Option[String] = Some("latest")) {

  override def toString: String = version match {
    case Some(v) => s"$name:$v"
    case None => name
  }

  def server: Option[String] = Tag.extract(toString, "server")

  def host: Option[String] = Tag.extract(toString, "host")

  def port: Option[String] = Tag.extract(toString, "port")

  def path: Option[String] = Tag.extract(toString, "path")
}

object Tag {

  val tagExpr = new Regex("""^((([a-z][a-z0-9-_\.]*)(:([0-9]+))?)(/[a-z0-9-_]+)*)(:([a-z0-9-_]+))?$""",
    "name", "server", "host", "", "port", "path", "", "version")

  private def extract(str: String, groupName: String): Option[String] = {
    tagExpr.findFirstMatchIn(str).map(_.group(groupName))
  }

  def fromString(str: String): Tag = {
    tagExpr.findFirstMatchIn(str) match {
      case None => throw new IllegalArgumentException(s"Invalid tag expression: $str")
      case Some(m) =>
        val version = m.group("version")
        new Tag(m.group("name"), if (version.isEmpty) None else Some(version))
    }
  }
}
