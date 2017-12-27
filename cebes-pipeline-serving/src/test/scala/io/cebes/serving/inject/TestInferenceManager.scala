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
package io.cebes.serving.inject

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.google.inject.Inject
import io.cebes.pipeline.InferenceManager
import io.cebes.pipeline.factory.PipelineFactory
import io.cebes.serving.common.DefaultPipelineJsonProtocol._
import io.cebes.serving.common.ServingActor
import io.cebes.util.ResourceUtil

import scala.concurrent.{ExecutionContextExecutor, Future}

class TestInferenceManager @Inject()(private val pplFactory: PipelineFactory,
                                     private val servingActor: ServingActor) extends InferenceManager {

  private implicit val actorSystem: ActorSystem = servingActor.actorSystem
  private implicit val actorExecutor: ExecutionContextExecutor = servingActor.actorExecutor
  private implicit val actorMaterializer: ActorMaterializer = servingActor.actorMaterializer

  private lazy val servings: Map[String, Future[PipelineInformation]] = loadTestPipelines()

  override def getPipeline(servingName: String): Future[PipelineInformation] = {
    servings.get(servingName) match {
      case None => throw new IllegalArgumentException(s"Serving name not found: $servingName")
      case Some(futurePplInfo) => futurePplInfo
    }
  }

  private def loadTestPipelines(): Map[String, Future[PipelineInformation]] = {
    val ftPplInfo = Future {
      val linearRegressionZip = ResourceUtil.getResourceAsFile("/exported-pipelines/linear_regression.zip")
      val ppl = pplFactory.importZip(linearRegressionZip.toString)
      PipelineInformation(ppl, Map("s1" -> "s1"))
    }
    Map("testLinearRegression" -> ftPplInfo)
  }
}
