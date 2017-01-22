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
 * Created by phvu on 13/12/2016.
 */

package io.cebes.server.routes.df

import com.google.inject.Inject
import io.cebes.df.{Dataframe, DataframeService}
import io.cebes.server.result.ResultStorage
import io.cebes.server.routes.common.AsyncDataframeOperation

import scala.concurrent.{ExecutionContext, Future}

/**
  * Runs the given SQL string, and returns a [[Dataframe]]
  */
class Sql @Inject()(dfService: DataframeService, override val resultStorage: ResultStorage)
  extends AsyncDataframeOperation[String] {

  override protected def runImpl(requestEntity: String)(implicit ec: ExecutionContext): Future[Dataframe] = Future {
    dfService.sql(requestEntity)
  }
}