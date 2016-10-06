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
 * Created by phvu on 26/09/16.
 */

package io.cebes.df.schema

object StorageTypes {

  sealed abstract class StorageType(val name: String) {
    override def toString = name
  }

  case object STRING extends StorageType("String")

  case object BOOLEAN extends StorageType("Boolean")
  case object BYTE extends StorageType("Byte")
  case object SHORT extends StorageType("Short")
  case object INT extends StorageType("Int")
  case object LONG extends StorageType("Long")
  case object FLOAT extends StorageType("Float")
  case object DOUBLE extends StorageType("Double")

  case object VECTOR extends StorageType("Vector")
  case object BINARY extends StorageType("Binary")

  case object DATE extends StorageType("Date")
  case object TIMESTAMP extends StorageType("Timestamp")

  val values = Seq(STRING, BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, VECTOR, BINARY, DATE, TIMESTAMP)

  def fromString(name: String): StorageType = values.find(_.name == name) match {
    case Some(t) => t
    case None => throw new IllegalArgumentException(s"Unrecognized column type: $name")
  }
}
