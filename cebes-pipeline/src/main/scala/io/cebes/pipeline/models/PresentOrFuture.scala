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
 */
package io.cebes.pipeline.models

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

private[models] object PresentOrFuture {

  def apply[T](value: T): PresentOrFuture[T] = Present(value)

  def apply[T](futureVal: Future[T]): PresentOrFuture[T] = FutureValue(futureVal)
}

/**
  * A trait that holds an object of either a Future[T] or a T
  * with functions to take either the present value or the future value,
  * regardless of what kind the actual value is.
  * This is designed to handle Future transparently for the slots.
  */
private[models] trait PresentOrFuture[+T] {

  def get: T

  def getFuture(implicit ec: ExecutionContext): Future[T]

}

private case class Present[+T](private val value: T) extends PresentOrFuture[T] {

  override def get: T = value

  override def getFuture(implicit ec: ExecutionContext): Future[T] = Future(value)
}

private case class FutureValue[+T](private val futureVal: Future[T]) extends PresentOrFuture[T] {

  override def get: T = Await.result(futureVal, Duration.Inf)

  override def getFuture(implicit ec: ExecutionContext): Future[T] = futureVal
}
