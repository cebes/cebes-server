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
 * Created by phvu on 05/09/16.
 */

package io.cebes.server.routes.test

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import io.cebes.server.http.SecuredSession
import io.cebes.server.routes.common.OperationHelper
import io.cebes.server.routes.test.HttpTestJsonProtocol._

trait TestHandler extends SecuredSession with OperationHelper {

  val testApi: Route = pathPrefix("test") {
    myRequiredSession { _ =>
      operation[LoadData, LoadDataRequest, LoadDataResponse]
    }
  }
}
