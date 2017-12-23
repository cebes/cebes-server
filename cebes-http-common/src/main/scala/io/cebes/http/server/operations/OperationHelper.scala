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
package io.cebes.http.server.operations

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{RequestContext, Route}
import akka.http.scaladsl.unmarshalling.FromRequestUnmarshaller
import io.cebes.http.server.HttpJsonProtocol._
import io.cebes.http.server.routes.{AkkaImplicits, HasInjector}
import spray.json.JsonFormat

import scala.reflect.ClassTag

/**
  * Convenient functions for working with generic operations
  */
trait OperationHelper extends AkkaImplicits with HasInjector {

  /**
    * An operation done by class [[W]] (subclass of [[AsyncOperation]],
    * with entity of type [[E]] and result of type [[R]]
    */
  protected def operation[W <: AsyncOperation[E, _, R], E, R]
  (implicit tag: ClassTag[W], umE: FromRequestUnmarshaller[E],
   jfE: JsonFormat[E], jfR: JsonFormat[R]): Route = {
    val workerClass = tag.runtimeClass.asInstanceOf[Class[W]]
    val workerName = workerClass.getSimpleName.toLowerCase
    (path(workerName) & post) {
      entity(as[E]) { requestEntity =>
        implicit ctx: RequestContext =>
          injector.getInstance(workerClass).run(requestEntity).flatMap(ctx.complete(_))
      }
    }
  }
}
