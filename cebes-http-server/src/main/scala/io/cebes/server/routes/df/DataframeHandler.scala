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

import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{RequestContext, Route}
import akka.http.scaladsl.unmarshalling.FromRequestUnmarshaller
import akka.stream.Materializer
import com.typesafe.scalalogging.LazyLogging
import io.cebes.common.Tag
import io.cebes.df.sample.DataSample
import io.cebes.server.http.SecuredSession
import io.cebes.server.inject.CebesHttpServerInjector
import io.cebes.server.routes.DataframeResponse
import io.cebes.server.routes.common.{AsyncDataframeOperation, AsyncOperation}
import io.cebes.server.routes.df.HttpDfJsonProtocol._
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

  val dataframeApi: Route = pathPrefix("df") {
    myRequiredSession { _ =>
      concat(
        operationDf[Sql, String],

        operationDf[TagAdd, TagAddRequest],
        operationDf[TagDelete, TagDeleteRequest],
        operation[Tags, TagsGetRequest, Array[(Tag, UUID)]],
        operationDf[Get, String],

        operationDf[InferVariableTypes, LimitRequest],
        operationDf[WithVariableTypes, WithVariableTypesRequest],
        operation[Count, DataframeRequest, Long],
        operationDf[Sample, SampleRequest],
        operation[Take, LimitRequest, DataSample],

        operationDf[Sort, ColumnsRequest],
        operationDf[DropColumns, ColumnNamesRequest],
        operationDf[DropDuplicates, ColumnNamesRequest],
        operationDf[DropNA, DropNARequest],
        operationDf[FillNA, FillNARequest],
        operationDf[FillNAWithMap, FillNAWithMapRequest],
        operationDf[Replace, ReplaceRequest],

        operation[ApproxQuantile, ApproxQuantileRequest, Array[Double]],
        operation[Cov, ColumnNamesRequest, Double],
        operation[Corr, ColumnNamesRequest, Double],
        operationDf[Crosstab, ColumnNamesRequest],
        operationDf[FreqItems, FreqItemsRequest],
        operationDf[SampleBy, SampleByRequest],

        operationDf[WithColumn, WithColumnRequest],
        operationDf[WithColumnRenamed, WithColumnRenamedRequest],
        operationDf[Select, ColumnsRequest],
        operationDf[Where, ColumnsRequest],
        operationDf[Alias, AliasRequest],
        operationDf[Join, JoinRequest],
        operationDf[Limit, LimitRequest],
        operationDf[Union, DataframeSetRequest],
        operationDf[Intersect, DataframeSetRequest],
        operationDf[Except, DataframeSetRequest],
        operationDf[Broadcast, DataframeRequest],

        operationDf[Aggregate, AggregateRequest]
      )
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  // Helpers
  /////////////////////////////////////////////////////////////////////////////

  /**
    * An operation done by class [[W]] (subclass of [[AsyncDataframeOperation]],
    * with entity of type [[E]]
    */
  private def operationDf[W <: AsyncDataframeOperation[E], E]
  (implicit tag: ClassTag[W], umE: FromRequestUnmarshaller[E], jfE: JsonFormat[E]): Route = {
    operation[W, E, DataframeResponse]
  }

  /**
    * An operation done by class [[W]] (subclass of [[AsyncOperation]],
    * with entity of type [[E]] and result of type [[R]]
    */
  private def operation[W <: AsyncOperation[E, _, R], E, R]
  (implicit tag: ClassTag[W], umE: FromRequestUnmarshaller[E], jfE: JsonFormat[E], jfR: JsonFormat[R]): Route = {
    val workerName = tag.runtimeClass.asInstanceOf[Class[W]].getSimpleName.toLowerCase
    (path(workerName) & post) {
      entity(as[E]) { requestEntity =>
        implicit ctx: RequestContext =>
          CebesHttpServerInjector.instance[W].run(requestEntity).flatMap(ctx.complete(_))
      }
    }
  }
}
