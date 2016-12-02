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

package io.cebes.server.result

import java.util.UUID

import spray.json._
import io.cebes.server.helpers.TestInjector
import io.cebes.server.models.{RequestStatus, SerializableResult}
import org.scalatest.FunSuite

class JdbcResultStorageSuite extends FunSuite {

  test("save and get") {
    val jdbcStorage = TestInjector.instance(classOf[JdbcResultStorage])

    val requestId = UUID.randomUUID()
    assert(jdbcStorage.get(requestId).isEmpty)

    jdbcStorage.save(SerializableResult(requestId, RequestStatus.SCHEDULED,
      Some("""{ "some": "JSON source" }""".parseJson)))
    val result = jdbcStorage.get(requestId)
    assert(result.nonEmpty)
    assert(result.get.status === RequestStatus.SCHEDULED)
    assert(result.get.requestId === requestId)
    assert(result.get.response.nonEmpty)
    assert(result.get.response.get.prettyPrint.length > 0)

    // replace
    jdbcStorage.save(SerializableResult(requestId, RequestStatus.FAILED, None))
    val result2 = jdbcStorage.get(requestId)
    assert(result2.nonEmpty)
    assert(result2.get.status === RequestStatus.FAILED)
    assert(result2.get.requestId === requestId)
    assert(result2.get.response.isEmpty)

    jdbcStorage.remove(requestId)
    assert(jdbcStorage.get(requestId).isEmpty)
  }

  test("empty json response") {
    val jdbcStorage = TestInjector.instance(classOf[JdbcResultStorage])

    val requestId = UUID.randomUUID()
    assert(jdbcStorage.get(requestId).isEmpty)

    jdbcStorage.save(SerializableResult(requestId, RequestStatus.SCHEDULED, None))
    val result = jdbcStorage.get(requestId)
    assert(result.nonEmpty)
    assert(result.get.status === RequestStatus.SCHEDULED)
    assert(result.get.requestId === requestId)
    assert(result.get.response.isEmpty)

    jdbcStorage.remove(requestId)
    assert(jdbcStorage.get(requestId).isEmpty)
  }
}