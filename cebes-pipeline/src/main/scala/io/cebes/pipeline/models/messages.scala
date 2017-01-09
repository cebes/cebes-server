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

import io.cebes.pipeline.protos.message.{DataframeMessageDef, ModelMessageDef, PipelineMessageDef, SampleMessageDef}
import io.cebes.pipeline.protos.value.ValueDef

trait PipelineMessage

class DataframeMessage extends PipelineMessage

class SampleMessage extends PipelineMessage

class ModelMessage extends PipelineMessage

class ValueMessage extends PipelineMessage

class PipelineMessageFactory {

  private def createDf(proto: DataframeMessageDef): DataframeMessage = {
    throw new NotImplementedError()
  }

  private def createDfSample(proto: SampleMessageDef): SampleMessage = {
    throw new NotImplementedError()
  }

  private def createModel(proto: ModelMessageDef): ModelMessage = {
    throw new NotImplementedError()
  }

  private def createValue(proto: ValueDef): ValueMessage = {
    throw new NotImplementedError()
  }

  def create(proto: PipelineMessageDef): PipelineMessage = {
    if (proto.msg.isDf) {
      createDf(proto.getDf)
    } else if (proto.msg.isDfSample) {
      createDfSample(proto.getDfSample)
    } else if (proto.msg.isModel) {
      createModel(proto.getModel)
    } else if (proto.msg.isValue) {
      createValue(proto.getValue)
    } else {
      throw new IllegalArgumentException(s"Unsupported PipelineMessageDef: ${proto.toString}")
    }
  }
}

