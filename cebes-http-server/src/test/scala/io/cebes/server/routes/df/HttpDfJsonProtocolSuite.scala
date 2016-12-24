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
 * Created by phvu on 24/12/2016.
 */

package io.cebes.server.routes.df

import java.util.UUID

import io.cebes.df.Column
import io.cebes.df.expressions.{Literal, Pow}
import io.cebes.spark.df.expressions.SparkPrimitiveExpression
import org.scalatest.FunSuite
import spray.json._
import io.cebes.server.routes.df.HttpDfJsonProtocol._

class HttpDfJsonProtocolSuite extends FunSuite {

  test("JSON Serialization Column") {
    val col = new Column(Pow(SparkPrimitiveExpression(UUID.randomUUID(), "col_blah", None), Literal(200)))
    val colStr = col.toJson.compactPrint

    val col2 = colStr.parseJson.convertTo[Column]
    assert(col2.expr === col.expr)
  }
}
