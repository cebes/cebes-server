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
 * Created by phvu on 30/11/2016.
 */

package io.cebes.http.server.result

import java.util.UUID

import io.cebes.http.inject.CebesHttpCommonTestInjector
import io.cebes.http.server.jdbc.JdbcResultStorage
import io.cebes.http.server.{RequestStatuses, SerializableResult}
import org.scalatest.FunSuite
import spray.json._

class InMemoryResultStorageSuite extends FunSuite {

  test("save and get") {
    val jdbcStorage = CebesHttpCommonTestInjector.instance[InMemoryResultStorage]

    val requestId = UUID.randomUUID()
    assert(jdbcStorage.get(requestId).isEmpty)

    jdbcStorage.save(SerializableResult(requestId, "sampleUri", Some("""{ "some": "JSON source" }""".parseJson),
      RequestStatuses.SCHEDULED, Some("""{ "some": "request" }""".parseJson)))
    val result = jdbcStorage.get(requestId)
    assert(result.nonEmpty)
    assert(result.get.status === RequestStatuses.SCHEDULED)
    assert(result.get.requestId === requestId)
    assert(result.get.response.nonEmpty)
    assert(result.get.response.get.prettyPrint.length > 0)
    assert(result.get.requestEntity.nonEmpty)

    // replace
    jdbcStorage.save(SerializableResult(requestId, "sampleUri", None, RequestStatuses.FAILED, None))
    val result2 = jdbcStorage.get(requestId)
    assert(result2.nonEmpty)
    assert(result2.get.status === RequestStatuses.FAILED)
    assert(result2.get.requestId === requestId)
    assert(result2.get.response.isEmpty)
    assert(result2.get.requestEntity.isEmpty)
  }

  test("empty json response") {
    val jdbcStorage = CebesHttpCommonTestInjector.instance[JdbcResultStorage]

    val requestId = UUID.randomUUID()
    assert(jdbcStorage.get(requestId).isEmpty)

    jdbcStorage.save(SerializableResult(requestId, "sampleUri", None, RequestStatuses.SCHEDULED, None))
    val result = jdbcStorage.get(requestId)
    assert(result.nonEmpty)
    assert(result.get.status === RequestStatuses.SCHEDULED)
    assert(result.get.requestId === requestId)
    assert(result.get.response.isEmpty)
  }

}
