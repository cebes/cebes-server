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
 * Created by phvu on 19/09/16.
 */

package io.cebes.server.routes.result

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives._
import io.cebes.server.http.SecuredSession
import io.cebes.server.models.CebesJsonProtocol._
import io.cebes.server.routes.common.HandlerHelpers

import scala.concurrent.ExecutionContext

trait ResultHandler extends SecuredSession with HandlerHelpers {

  implicit def actorExecutor: ExecutionContext

  val resultApi = pathPrefix("request") {
    (path(JavaUUID) & post) { requestId =>
      myRequiredSession { session =>
        implicit ctx => ctx.complete(instance(classOf[Result]).run(requestId))
      }
    }
  }
}
