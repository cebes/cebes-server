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
 */
package io.cebes.serving.routes

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import io.cebes.http.server.operations.OperationHelper
import io.cebes.http.server.routes.AkkaImplicits
import io.cebes.pipeline.InferenceService
import io.cebes.pipeline.json.{InferenceRequest, InferenceResponse}
import io.cebes.serving.DefaultPipelineJsonProtocol._

trait InferenceHandler extends AkkaImplicits with OperationHelper {

  protected val inferenceService: InferenceService

  protected val inferenceApi: Route =
    concat(operation[Inference, InferenceRequest, InferenceResponse],
      (path("inferenceSync") & post) {
        entity(as[InferenceRequest]) { formData =>
          extractExecutionContext { implicit executor =>
            implicit ctx =>
              injector.getInstance(classOf[InferenceSync]).run(formData).flatMap(ctx.complete(_))
          }
        }
      }
    )
}
