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
import io.cebes.df.DataframeService
import io.cebes.http.server.operations.AsyncSerializableOperation
import io.cebes.http.server.result.ResultStorage

import scala.concurrent.{ExecutionContext, Future}

/**
  * Compute the covariance between two columns
  */
class Cov @Inject()(dfService: DataframeService, override val resultStorage: ResultStorage)
  extends AsyncSerializableOperation[ColumnNamesRequest, Double] {

  /**
    * Implement this to do the real work
    */
  override protected def runImpl(requestEntity: ColumnNamesRequest)
                                (implicit ec: ExecutionContext): Future[Double] = Future {
    require(requestEntity.colNames.length == 2,
      s"Cov requires 2 column names, got ${requestEntity.colNames.length} columns")
    dfService.cov(requestEntity.df, requestEntity.colNames.head, requestEntity.colNames.last)
  }
}
