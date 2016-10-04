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

object ColumnTypes {

  sealed abstract class ColumnType(val name: String) {
    override def toString = name
  }

  case object STRING extends ColumnType("String")

  case object BOOLEAN extends ColumnType("Boolean")
  case object BYTE extends ColumnType("Byte")
  case object SHORT extends ColumnType("Short")
  case object INT extends ColumnType("Int")
  case object LONG extends ColumnType("Long")
  case object FLOAT extends ColumnType("Float")
  case object DOUBLE extends ColumnType("Double")

  case object VECTOR extends ColumnType("Vector")
  case object BINARY extends ColumnType("Binary")

  case object DATE extends ColumnType("Date")
  case object TIMESTAMP extends ColumnType("Timestamp")

  val values = Seq(STRING, BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, VECTOR, BINARY, DATE, TIMESTAMP)

  def fromString(name: String): ColumnType = values.find(_.name == name) match {
    case Some(t) => t
    case None => throw new IllegalArgumentException(s"Unrecognized column type: $name")
  }
}
