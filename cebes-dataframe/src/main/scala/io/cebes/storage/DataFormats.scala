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

object DataFormats {
  sealed abstract class DataFormat(val name: String) {
    override def toString: String = name
  }

  case object UNKNOWN extends DataFormat("unknown")
  case object CSV extends DataFormat("csv")
  case object JSON extends DataFormat("json")
  case object PARQUET extends DataFormat("parquet")
  case object ORC extends DataFormat("orc")
  case object TEXT extends DataFormat("text")

  val values = Seq(UNKNOWN, CSV, JSON, PARQUET, ORC, TEXT)

  def fromString(name: String) = values.find(_.name == name)
}
