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

import java.util.concurrent.Executors

import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.concurrent.ExecutionContext

/**
  * Provides implicit ExecutionContext for tests messing with Future.
  * Future(s) that involve Spark operations shouldn't be called with
  * [[scala.concurrent.ExecutionContext.Implicits.global]]
  * since it will mess up with some weird Reflection bugs in hadoop
  * (although I still don't know why).
  * Those tests should extends this trait instead
  */
trait ImplicitExecutor extends BeforeAndAfterAll { this: Suite =>

  private val executorService = Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors())
  protected implicit val testContext: ExecutionContext = ExecutionContext.fromExecutor(executorService)

  override def afterAll(): Unit = {
    super.afterAll()
    executorService.shutdown()
  }
}
