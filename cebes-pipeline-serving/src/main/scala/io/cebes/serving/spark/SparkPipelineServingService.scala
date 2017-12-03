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
package io.cebes.serving.spark

import com.google.inject.Inject
import io.cebes.pipeline.models.{PipelineMessageSerializer, SlotDescriptor}
import io.cebes.serving.common.ServingManager
import io.cebes.serving.{InferenceRequest, InferenceResponse, PipelineServingService}

import scala.concurrent.{ExecutionContext, Future}

class SparkPipelineServingService @Inject()(private val servingManager: ServingManager,
                                            private val pplMessageSerializer: PipelineMessageSerializer)
  extends PipelineServingService {

  override def inference(request: InferenceRequest)(implicit executor: ExecutionContext): Future[InferenceResponse] = {
    servingManager.getPipeline(request.servingName).flatMap { pplInfo =>

      // deserialize the request, prepare inputs for pipeline's run()
      // TODO: take care SampleMessageDef during deserialization
      // also needs to check the sample with the stored data schema in that slot
      val outs = request.outputs.map(s => SlotDescriptor(s))
      val feeds = request.inputs.map { case (slotName, slotValue) =>
        val feedInputSlot = SlotDescriptor(pplInfo.slotNamings.getOrElse(slotName, slotName))
        val value = pplMessageSerializer.deserialize(slotValue)
        feedInputSlot -> value
      }

      pplInfo.pipeline.run(outs, feeds).map { outputs =>
        val results = outputs.map { case (slotDesc, value) =>
          s"${slotDesc.parent}:${slotDesc.name}" -> pplMessageSerializer.serialize(value)
        }
        InferenceResponse(results)
      }
    }
  }
}
