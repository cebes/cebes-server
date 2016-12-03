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
 * Created by phvu on 07/11/2016.
 */

package io.cebes.server.routes.result

import java.util.UUID

import akka.http.scaladsl.server.RequestContext
import com.google.inject.Inject
import io.cebes.server.models.SerializableResult
import io.cebes.server.result.ResultStorage
import io.cebes.server.routes.common.SyncExecutor

import scala.concurrent.{ExecutionContext, Future}

class Result @Inject()(resultStorage: ResultStorage) extends SyncExecutor[UUID, SerializableResult] {

  /**
    * Implement this to do the real work
    */
  override def run(requestEntity: UUID)
                  (implicit ec: ExecutionContext,
                   ctx: RequestContext): Future[SerializableResult] = Future {
    resultStorage.get(requestEntity) match {
      case Some(result) => result
      case None => throw new NoSuchElementException(s"Request ID not found: ${requestEntity.toString}")
    }
  }
}
