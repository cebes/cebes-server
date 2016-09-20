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
import io.cebes.server.result.ResultStorage

trait ResultHandler extends SecuredSession {

  val resultStorage: ResultStorage

  val resultApi = pathPrefix("request") {
    myRequiredSession { session =>
      pathPrefix(JavaUUID) { requestId =>
        post { ctx =>
          resultStorage.get(requestId) match {
            case Some(result) => ctx.complete(result)
            case None => throw new NoSuchElementException(s"Request ID not found: ${requestId.toString}")
          }
        }
      }
    }
  }
}
