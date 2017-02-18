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

import scala.concurrent.{ExecutionContext, Future}

/**
  * Abstraction of inputs to a [[Stage]], of type T
  * which can be either an [[OrdinaryInput]] or a [[StageOutput]]
  */
private[models] trait StageInput[+T] {

  def get: T

  def getFuture(implicit ec: ExecutionContext): Future[T]

}

private[models] object StageInput {

  def apply[T](value: T): StageInput[T] = OrdinaryInput(value)

}

private case class OrdinaryInput[+T](private val value: T) extends StageInput[T] {

  override def get: T = value

  override def getFuture(implicit ec: ExecutionContext): Future[T] = Future(value)
}

/**
  * An output of a [[Stage]], which can be fed into another stage (because
  * it is a subclass of [[StageInput]], or can be waited on (via the [[getFuture]] function)
  * to get the final result.
  * The purpose of this class is to delay the computation of the actual output (in [[getFuture]])
  * to the point where it is actually needed.
  */
case class StageOutput[+T](stage: Stage, outputName: String) extends StageInput[T] {

  @volatile private var isNew: Boolean = true

  def isNewOutput: Boolean = isNew || stage.nonDeterministic

  def newOutput(): Unit = {
    isNew = true
  }

  def seen(): Unit = {
    isNew = false
  }

  override def get: T = {
    throw new UnsupportedOperationException("Getting the actual value of a StageOutput is unsupported. " +
      s"Use Await.result(getFuture(), ...) instead. Output $outputName of stage ${stage.toString}")
  }

  override def getFuture(implicit ec: ExecutionContext): Future[T] = stage.computeOutput(outputName)
}