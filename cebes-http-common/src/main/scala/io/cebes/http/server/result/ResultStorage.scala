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
 * Created by phvu on 17/09/16.
 */

package io.cebes.http.server.result

import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.Scheduler
import io.cebes.http.Retries
import io.cebes.http.server.SerializableResult

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

/**
  * A result storage.
  *
  * Classes implemented this trait should use the @Singleton annotation (to be instantiated by the DI framework)
  * and make sure that they are thread-safe.
  */
trait ResultStorage {

  /**
    * Save the serializable result
    */
  def save(serializableResult: SerializableResult): Unit

  /**
    * Save the result, and retry for several times if it fails
    */
  def saveWithRetry(serializableResult: SerializableResult)
                   (implicit ec: ExecutionContext, s: Scheduler): Future[Unit] = {
    Retries.retry(Retries.constants(5, FiniteDuration(2, TimeUnit.SECONDS)))(save(serializableResult))
  }

  /**
    * Get the saved serializable result of the given request ID
    */
  def get(requestId: UUID): Option[SerializableResult]
}
