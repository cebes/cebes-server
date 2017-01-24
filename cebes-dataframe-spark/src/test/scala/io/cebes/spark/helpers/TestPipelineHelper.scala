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
package io.cebes.spark.helpers

import java.util.concurrent.TimeUnit

import io.cebes.df.Dataframe
import io.cebes.pipeline.models.{DataframeMessage, PipelineMessage}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

trait TestPipelineHelper {

  private val waitTime = Duration(2, TimeUnit.MINUTES)

  /** Generic wait function for getting a Pipeline message */
  protected def result(awaitable: => Future[PipelineMessage]): PipelineMessage = Await.result(awaitable, waitTime)

  /** Specialized wait function for results that are [[DataframeMessage]] */
  protected def resultDf(waitable: => Future[PipelineMessage]): Dataframe = {
    val r = result(waitable)
    assert(r.isInstanceOf[DataframeMessage])
    r.asInstanceOf[DataframeMessage].df
  }
}
