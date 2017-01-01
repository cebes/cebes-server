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
 * Created by phvu on 30/12/2016.
 */

package io.cebes.persistence

import java.util.UUID

import org.scalatest.FunSuite

case class Foo(field1: Int, field2: Float)

class InMemoryPersistenceSuite extends FunSuite {

  test("persistence with UUID key") {
    val persistence = new InMemoryPersistence[UUID, Foo]()
    val key = UUID.randomUUID()

    persistence.get(key) match {
      case Some(_) => persistence.remove(key)
      case None =>
    }
    assert(persistence.get(key).isEmpty)

    persistence.insert(key, Foo(300, 219.0f))
    val v0 = persistence.get(key)
    assert(v0.nonEmpty)
    assert(v0.get.field1 === 300)
    assert(v0.get.field2 === 219.0f)

    // insert the same key
    val ex = intercept[IllegalArgumentException] {
      persistence.insert(key, Foo(100, 219.0f))
    }
    assert(ex.getMessage.startsWith("Duplicated key"))

    // find values
    val keys1 = persistence.findValue(Foo(300, 219.0f))
    val seq1 = keys1.toSeq
    assert(seq1.length === 1)
    assert(seq1.head === key)
    keys1.close()

    val keys2 = persistence.findValue(Foo(100, 219.1f))
    assert(keys2.isEmpty)
    keys2.close()

    // elements
    val elements = persistence.elements
    assert(elements.size === 1)
    elements.close()

    // replace
    persistence.upsert(key, Foo(-100, -219.0f))
    val v2 = persistence.get(key)
    assert(v2.nonEmpty)
    assert(v2.get.field1 === -100)
    assert(v2.get.field2 === -219.0f)

    persistence.remove(key)
    assert(persistence.get(key).isEmpty)
  }
}
