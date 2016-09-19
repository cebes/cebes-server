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
 *
 * Created by phvu on 19/09/16.
 */

package io.cebes.server.result

import java.util.UUID

import io.cebes.server.models.Result
import spray.json._

case class SerializableResult(requestId: UUID, result: JsValue)

object SerializableResult {

  def apply[E, R](result: Result[E, R])
                 (implicit jfE: JsonFormat[E],
                  jfR: JsonFormat[R],
                  jfResult: JsonFormat[Result[E, R]]): SerializableResult =
    new SerializableResult(result.request.requestId,
      result.response.toJson)
}
