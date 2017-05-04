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
package io.cebes.server.routes.pipeline

import io.cebes.pipeline.json.PipelineJsonProtocol
import io.cebes.server.routes.HttpJsonProtocol
import io.cebes.server.routes.common.HttpTagJsonProtocol

trait HttpPipelineJsonProtocol extends HttpJsonProtocol with HttpTagJsonProtocol with PipelineJsonProtocol {

}

object HttpPipelineJsonProtocol extends HttpPipelineJsonProtocol
