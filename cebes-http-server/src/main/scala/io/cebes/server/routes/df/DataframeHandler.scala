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

package io.cebes.server.routes.df

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{RequestContext, Route}
import akka.http.scaladsl.unmarshalling.FromRequestUnmarshaller
import akka.stream.Materializer
import com.typesafe.scalalogging.LazyLogging
import io.cebes.server.http.SecuredSession
import io.cebes.server.inject.CebesHttpServerInjector
import io.cebes.server.routes.common.AsyncDataframeOperation
import io.cebes.server.routes.df.CebesDfProtocol._
import spray.json._

import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag

/**
  * Handle all requests related to dataframe
  */
trait DataframeHandler extends SecuredSession with LazyLogging {

  implicit def actorSystem: ActorSystem

  implicit def actorExecutor: ExecutionContext

  implicit def actorMaterializer: Materializer

  /**
    * An operation done by class [[W]] (subclass of [[AsyncDataframeOperation]],
    * with entity of type [[E]]
    */
  private def operation[W <: AsyncDataframeOperation[E], E](implicit umE: FromRequestUnmarshaller[E],
                                                            jfE: JsonFormat[E], tag: ClassTag[W]): Route = {
    val workerName = tag.runtimeClass.asInstanceOf[Class[W]].getSimpleName.toLowerCase
    (path(workerName) & post) {
      entity(as[E]) { requestEntity =>
        implicit ctx: RequestContext =>
          CebesHttpServerInjector.instance[W].run(requestEntity).flatMap(ctx.complete(_))
      }
    }
  }

  val dataframeApi: Route = pathPrefix("df") {
    myRequiredSession { _ =>
      concat(
        operation[Sample, SampleRequest],
        operation[Sql, String]
      )
    }
  }
}
