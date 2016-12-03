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

package io.cebes.server.result

import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.Scheduler
import akka.pattern.after
import io.cebes.server.models.SerializableResult

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
    ResultStorage.retry(save(serializableResult))
  }

  /**
    * Get the saved serializable result of the given request ID
    */
  def get(requestId: UUID): Option[SerializableResult]
}

object ResultStorage {

  val retryDuration = FiniteDuration(2, TimeUnit.SECONDS)
  val retryCount = 5

  def retry[T](op: => T)(implicit ec: ExecutionContext, s: Scheduler): Future[T] =
    retry(ResultStorage.retryDuration, ResultStorage.retryCount)(op)

  /**
    * Given an operation that produces a T, returns a Future containing the result of T,
    * unless an exception is thrown, in which case the operation will be retried after _delay_ time,
    * if there are more possible retries, which is configured through the _retries_ parameter.
    * If the operation does not succeed and there is no retries left,
    * the resulting Future will contain the last failure.
    **/
  def retry[T](delay: FiniteDuration, retries: Int)(op: => T)
              (implicit ec: ExecutionContext, s: Scheduler): Future[T] =
    Future(op).recoverWith {
      case _ if retries > 0 => after(delay, s)(retry(delay, retries - 1)(op))
    }
}
