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
package io.cebes.server.routes.test

import io.cebes.server.routes.common.DataframeResponse
import io.cebes.server.routes.common.HttpServerJsonProtocol._
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

case class LoadDataRequest(datasets: Array[String])

case class LoadDataResponse(dataframes: Array[DataframeResponse])

trait HttpTestJsonProtocol extends DefaultJsonProtocol {

  implicit val loadDataRequestFormat: RootJsonFormat[LoadDataRequest] = jsonFormat1(LoadDataRequest)
  implicit val loadDataResponseFormat: RootJsonFormat[LoadDataResponse] = jsonFormat1(LoadDataResponse)

}

object HttpTestJsonProtocol extends HttpTestJsonProtocol
