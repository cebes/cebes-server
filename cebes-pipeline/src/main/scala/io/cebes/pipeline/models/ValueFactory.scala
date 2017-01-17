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

import io.cebes.df.Column
import io.cebes.json.CebesCoreJsonProtocol._
import io.cebes.pipeline.protos.value.{ScalarDef, ValueDef}
import spray.json._

/**
  * Created by d066177 on 14/01/2017.
  */
class ValueFactory {

  def fromProto(valueDef: ValueDef): Any = valueDef.value match {
    case ValueDef.Value.Scalar(scalarDef) =>
      scalarDef.value match {
        case ScalarDef.Value.StringVal(s) => s
        case ScalarDef.Value.BoolVal(b) => b
        case ScalarDef.Value.Int32Val(i) => i
        case ScalarDef.Value.Int64Val(l) => l
        case ScalarDef.Value.FloatVal(f) => f
        case ScalarDef.Value.DoubleVal(d) => d
        case scalarVal =>
          throw new UnsupportedOperationException("Unsupported scalar value: " +
            s"${scalarVal.toString} of type ${scalarVal.getClass.getName}")
      }
    case ValueDef.Value.Column(columnDef) =>
      columnDef.columnJson.parseJson.convertTo[Column]
    case ValueDef.Value.Array(arrayDef) =>
      arrayDef.element.map(fromProto).toArray[Any]
    case ValueDef.Value.Map(mapDef) =>
      mapDef.entry.filter(_.key.isDefined).map { entry =>
        fromProto(entry.key.get) -> entry.value.map(fromProto).orNull
      }.toMap[Any, Any]
    case v =>
      throw new UnsupportedOperationException("Unsupported value: " +
        s"${v.toString} of type ${v.getClass.getName}")
  }
}
