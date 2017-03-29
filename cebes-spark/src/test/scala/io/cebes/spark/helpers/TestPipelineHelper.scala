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
import io.cebes.pipeline.factory.PipelineFactory
import io.cebes.spark.CebesSparkTestInjector

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

trait TestPipelineHelper {

  lazy val pipelineFactory: PipelineFactory = CebesSparkTestInjector.instance[PipelineFactory]

  protected val TEST_WAIT_TIME = Duration(2, TimeUnit.MINUTES)

  /** Generic wait function for getting a Pipeline message */
  protected def result[T](awaitable: => Future[T]): T = Await.result(awaitable, TEST_WAIT_TIME)

  /** Specialized wait function for results that are [[Dataframe]] */
  protected def resultDf(waitable: => Future[Dataframe]): Dataframe = {
    val r = result(waitable)
    assert(r.isInstanceOf[Dataframe])
    r
  }
}
