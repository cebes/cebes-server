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
 * Created by phvu on 24/08/16.
 */

package io.cebes.server.http

import akka.http.scaladsl.server.Directives._
import io.cebes.server.auth.AuthHandler
import io.cebes.server.df.DataframeHandler

trait Routes extends ApiErrorHandler with AuthHandler with DataframeHandler {
  val routes =
    pathPrefix("v1") {
      authApi ~
      dataframeApi
    } ~ path("")(getFromResource("public/index.html"))
}
