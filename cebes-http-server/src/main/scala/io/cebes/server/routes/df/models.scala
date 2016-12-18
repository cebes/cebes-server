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
 * Created by phvu on 13/12/2016.
 */

package io.cebes.server.routes.df

import java.util.UUID

import io.cebes.server.routes.HttpJsonProtocol

case class SampleRequest(df: UUID, withReplacement: Boolean, fraction: Double, seed: Long)

case class TakeRequest(df: UUID, n: Int)

case class ColumnsRequest(df: UUID, columns: Array[String])

case class CountRequest(df: UUID)

trait HttpDfJsonProtocol extends HttpJsonProtocol {

  implicit val sampleRequestFormat = jsonFormat4(SampleRequest)
  implicit val takeRequestFormat = jsonFormat2(TakeRequest)
  implicit val columnsRequestFormat = jsonFormat2(ColumnsRequest)
  implicit val countRequestFormat = jsonFormat1(CountRequest)
}

object HttpDfJsonProtocol extends HttpDfJsonProtocol
