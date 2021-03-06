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
 * Created by phvu on 05/11/2016.
 */

package io.cebes.http.server.operations

import akka.http.scaladsl.server.RequestContext

import scala.concurrent.{ExecutionContext, Future}

/**
  * Trait designed for operations that need to be run synchronously
  *
  * @tparam E type of the request entity
  * @tparam R type of the result
  */
trait SyncOperation[E, R] {

  /**
    * Implement this to do the real work
    */
  def run(requestEntity: E)
         (implicit ec: ExecutionContext,
          ctx: RequestContext): Future[R]
}
