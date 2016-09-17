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
 * Created by phvu on 31/08/16.
 */

package io.cebes.storage

object DataFormat {
  sealed abstract class DataFormatEnum(val name: String) {
    override def toString: String = name
  }

  case object Unknown extends DataFormatEnum("unknown")
  case object Csv extends DataFormatEnum("csv")
  case object Json extends DataFormatEnum("json")
  case object Parquet extends DataFormatEnum("parquet")
  case object Orc extends DataFormatEnum("orc")
  case object Text extends DataFormatEnum("text")

  val values = Seq(Unknown, Csv, Json, Parquet, Orc, Text)

  def fromString(name: String) = values.find(_.name == name)
}
