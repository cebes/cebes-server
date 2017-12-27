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
package io.cebes.serving.common

import com.google.inject.Inject
import io.cebes.pipeline.json.{InferenceRequest, InferenceResponse}
import io.cebes.pipeline.models.SlotDescriptor
import io.cebes.pipeline.{InferenceManager, InferenceService}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Default implementation of [[InferenceService]]. This doesn't depend on Spark or any particular engine.
  */
class DefaultInferenceService @Inject()(protected val inferenceManager: InferenceManager,
                                        private val serializer: ServingPipelineSerializer)
  extends InferenceService {


  override def inference(request: InferenceRequest)
                        (implicit executor: ExecutionContext): Future[InferenceResponse] = {
    inferenceManager.getPipeline(request.servingName).flatMap { pplInfo =>

      // deserialize the request, prepare inputs for pipeline's run()
      val outs = request.outputs.map(s => SlotDescriptor(pplInfo.slotNamings.getOrElse(s, s)))
      val feeds = request.inputs.map { case (slotName, slotValue) =>
        val feedInputSlot = SlotDescriptor(pplInfo.slotNamings.getOrElse(slotName, slotName))
        feedInputSlot -> serializer.deserialize(pplInfo.pipeline, feedInputSlot, slotValue)
      }

      pplInfo.pipeline.run(outs, feeds).map { outputs =>
        val reversedNamings = pplInfo.slotNamings.map(_.swap)
        val results = outputs.map { case (slotDesc, value) =>
          val outSlotName = s"${slotDesc.parent}:${slotDesc.name}"
          reversedNamings.getOrElse(outSlotName, outSlotName) -> serializer.serialize(value, request.maxDfSize)
        }
        InferenceResponse(results)
      }
    }
  }
}
