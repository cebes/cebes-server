/* Copyright 2017 The Cebes Authors. All Rights Reserved.
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
package io.cebes.pipeline.inject

import java.util.UUID

import io.cebes.df.Dataframe
import io.cebes.pipeline.models.PipelineMessageSerializer

/**
  * Simple implementation of [[PipelineMessageSerializer]], that actually do nothing
  * Only for testing purpose
  */
class DummyPipelineMessageSerializer extends PipelineMessageSerializer {

  override protected def getDataframe(dfId: UUID): Dataframe = ???
}
