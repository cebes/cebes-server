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
 * Created by phvu on 17/12/2016.
 */

package io.cebes.df.types.storage

import spray.json._

trait MetadataJsonProtocol extends DefaultJsonProtocol {

  implicit object MetadataFormat extends JsonFormat[Metadata] {

    def read(json: JsValue): Metadata = json match {
      case obj: JsObject =>
        val builder = new MetadataBuilder
        obj.fields.foreach {
          case (key, JsNumber(value)) =>
            if (value.isValidLong) {
              builder.putLong(key, value.longValue())
            } else {
              builder.putDouble(key, value.doubleValue())
            }
          case (key, JsBoolean(value)) =>
            builder.putBoolean(key, value)
          case (key, JsString(value)) =>
            builder.putString(key, value)
          case (key, o: JsObject) =>
            builder.putMetadata(key, read(o))
          case (key, JsArray(value)) =>
            if (value.isEmpty) {
              // If it is an empty array, we cannot infer its element type. We put an empty Array[Long].
              builder.putLongArray(key, Array.emptyLongArray)
            } else {
              value.head match {
                case v: JsNumber if v.value.isValidLong =>
                  builder.putLongArray(key, value.map(_.asInstanceOf[JsNumber].value.longValue()).toArray)
                case _: JsNumber =>
                  builder.putDoubleArray(key, value.map(_.asInstanceOf[JsNumber].value.doubleValue()).toArray)
                case _: JsBoolean =>
                  builder.putBooleanArray(key, value.map(_.asInstanceOf[JsBoolean].value).toArray)
                case _: JsString =>
                  builder.putStringArray(key, value.map(_.asInstanceOf[JsString].value).toArray)
                case _: JsObject =>
                  builder.putMetadataArray(key, value.map(entry => read(entry.asInstanceOf[JsObject])).toArray)
                case other =>
                  serializationError(s"Do not support array of type ${other.getClass}.")
              }
            }
          case (key, JsNull) =>
            builder.putNull(key)
          case (key, other) => serializationError(s"Do not support type ${other.getClass} at key $key.")
        }
        builder.build()
      case other => serializationError(s"Expected a JsObject, but got ${other.getClass}")
    }

    def write(obj: Metadata): JsValue = {
      val fields = obj.map.map {
        case (k: String, v) => k -> jsonifyValue(v)
      }
      JsObject(fields)
    }

    private def jsonifyValue(v: Any): JsValue = v match {
      case metadata: Metadata => write(metadata)
      case arr: Array[_] => JsArray(arr.map(jsonifyValue): _*)
      case x: Long => JsNumber(x)
      case x: Double => JsNumber(x)
      case x: Boolean => JsBoolean(x)
      case x: String => JsString(x)
      case null => JsNull
      case other =>
        serializationError(s"Fail to serialize object of class ${other.getClass.getCanonicalName}")
    }
  }
}

object MetadataJsonProtocol extends MetadataJsonProtocol