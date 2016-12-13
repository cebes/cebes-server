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

package io.cebes.server.util

import java.util.concurrent.TimeUnit

import akka.actor.Scheduler
import akka.pattern.after

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

object Retries {

  /**
    * Retry with a sequence of delays
    *
    * Given an operation that produces a T, returns a Future containing the result of T,
    * unless an exception is thrown, in which case the operation
    * will be retried after durations specified in _delays_.
    * If the operation does not succeed and there is no retries left,
    * the resulting Future will contain the last failure.
    */
  def retry[T](delays: Seq[FiniteDuration])(op: => T)
              (implicit ec: ExecutionContext, s: Scheduler): Future[T] =
    Future(op).recoverWith {
      case _ if delays.nonEmpty => after(delays.head, s)(retry(delays.tail)(op))
    }

  /**
    * Retry until a condition is satisfied
    */
  def retryUntil[T](delays: Seq[FiniteDuration])(op: => T)(cond: T => Boolean)
                   (implicit ec: ExecutionContext, s: Scheduler): Future[T] = {
    retry(delays) {
      val result = op
      if (!cond(result)) {
        throw new RuntimeException("Condition was not satisfied")
      }
      result
    }
  }

  /**
    * Generate a sequence of _retries_ [[FiniteDuration]]s, with constant duration given in _delay_
    */
  def constants(retries: Int, delay: FiniteDuration): Seq[FiniteDuration] = (0 until retries).map(_ => delay)

  /**
    * Generate a sequence of _retries_ [[FiniteDuration]]s, using exponential-backoff
    * with the base of _delta_ milliseconds.
    * for i from 0 to retries:
    *   delta * (pow(2, rand(0, min(i, max_count))) - 1)
    *
    * @param delta in milliseconds
    * @param max_count the maximum value of the exponentials (cut-off value)
    * @return
    */
  def expBackOff(retries: Int = 10, delta: Long = 500, max_count: Int = 4): Seq[FiniteDuration] =
    (1 to retries).map { i =>
      val s = delta * ((1 << Random.nextInt(math.min(i, max_count))) - 1)
      FiniteDuration(s, TimeUnit.MILLISECONDS)
    }

}
