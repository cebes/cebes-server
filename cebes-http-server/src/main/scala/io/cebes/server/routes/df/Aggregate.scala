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
 * Created by phvu on 18/12/2016.
 */

package io.cebes.server.routes.df

import com.google.inject.Inject
import io.cebes.df.{Dataframe, DataframeService}
import io.cebes.http.server.result.ResultStorage
import io.cebes.server.routes.common.AsyncDataframeOperation

import scala.concurrent.{ExecutionContext, Future}

/**
  * Perform an aggregation, then optionally a pivot, then some actual computation.
  * Returns the final result as a [[Dataframe]].
  *
  * See the doc of [[AggregateRequest]] for more information on the arguments of this operation.
  */
class Aggregate @Inject()(dfService: DataframeService, override val resultStorage: ResultStorage)
  extends AsyncDataframeOperation[AggregateRequest] {

  override protected def runImpl(requestEntity: AggregateRequest)
                                (implicit ec: ExecutionContext): Future[Dataframe] = Future {
    requestEntity.genericAggExprs match {
      case Some(exprs) =>
        dfService.aggregateAgg(requestEntity.df, requestEntity.cols.toSeq, requestEntity.aggType,
          requestEntity.pivotColName, requestEntity.pivotValues.map(_.toSeq), exprs.toSeq)
      case None =>
        requestEntity.aggFunc match {
          case Some("count") =>
            dfService.aggregateCount(requestEntity.df, requestEntity.cols.toSeq, requestEntity.aggType,
              requestEntity.pivotColName, requestEntity.pivotValues.map(_.toSeq))
          case Some("min") =>
            dfService.aggregateMin(requestEntity.df, requestEntity.cols.toSeq, requestEntity.aggType,
              requestEntity.pivotColName, requestEntity.pivotValues.map(_.toSeq), requestEntity.aggColNames)
          case Some("mean") =>
            dfService.aggregateMean(requestEntity.df, requestEntity.cols.toSeq, requestEntity.aggType,
              requestEntity.pivotColName, requestEntity.pivotValues.map(_.toSeq), requestEntity.aggColNames)
          case Some("max") =>
            dfService.aggregateMax(requestEntity.df, requestEntity.cols.toSeq, requestEntity.aggType,
              requestEntity.pivotColName, requestEntity.pivotValues.map(_.toSeq), requestEntity.aggColNames)
          case Some("sum") =>
            dfService.aggregateSum(requestEntity.df, requestEntity.cols.toSeq, requestEntity.aggType,
              requestEntity.pivotColName, requestEntity.pivotValues.map(_.toSeq), requestEntity.aggColNames)
          case Some(other) =>
            throw new IllegalArgumentException(s"Unrecognized Aggregation function: $other. " +
              s"Valid values are count, min, mean, max, sum")
          case None =>
            throw new IllegalArgumentException(s"When aggregation expressions are not specified in " +
              s"`genericAggExprs`, you must provide an aggregation function in `aggFunc`")
        }
    }
  }
}
