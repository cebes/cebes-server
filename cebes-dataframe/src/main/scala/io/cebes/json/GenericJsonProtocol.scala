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
package io.cebes.json

import java.sql.Timestamp
import java.util.Date

import spray.json._

import scala.collection.mutable
import scala.reflect.ClassTag

/**
  * General helpers, for dealing with basic types
  */
trait GenericJsonProtocol {

  private def toJsObject(typeName: String, data: JsValue): JsObject = {
    JsObject(Map("type" -> JsString(typeName), "data" -> data))
  }

  /**
    * Write basic scala types to JSON
    */
  protected def writeJson(value: Any): JsValue = {
    value match {
      case null => JsNull
      case x: String => JsString(x)
      case x: Boolean => JsBoolean(x)
      case b: Byte => toJsObject("byte", JsNumber(b))
      case s: Short => toJsObject("short", JsNumber(s))
      case i: Int => toJsObject("int", JsNumber(i))
      case l: Long => toJsObject("long", JsNumber(l))
      case f: Float => toJsObject("float", JsNumber(f))
      case v: Number => toJsObject("double", JsNumber(v.doubleValue()))
      case t: Timestamp => toJsObject("timestamp", JsNumber(t.getTime))
      case d: Date => toJsObject("date", JsNumber(d.getTime))
      case arr: Array[_] =>
        toJsObject("array", JsArray(arr.map(writeJson): _*))
      case arr: mutable.WrappedArray[_] =>
        toJsObject("wrapped_array", JsArray(arr.map(writeJson): _*))
      case m: Map[_, _] =>
        val arr = JsArray(m.map {
          case (k, v) => JsObject(Map("key" -> writeJson(k), "val" -> writeJson(v)))
        }.toVector)
        toJsObject("map", arr)
      case seq: Seq[_] =>
        toJsObject("seq", JsArray(seq.map(writeJson): _*))
      case other =>
        serializationError(s"Don't know how to serialize values of class ${
          other.getClass.getName
        }")
    }
  }

  protected def asJsType[T <: JsValue](js: JsValue)(implicit tag: ClassTag[T]): T = js match {
    case v: T => v
    case other =>
      deserializationError(s"Expected a ${tag.runtimeClass.asInstanceOf[Class[T]].getName}, " +
        s"got ${other.getClass.getName}")
  }

  /**
    * Read the value written by [[writeJson()]]
    */
  protected def readJson(js: JsValue): Any = {
    js match {
      case JsNull => null
      case JsString(s) => s
      case JsBoolean(v) => v
      case obj: JsObject =>
        if (!obj.fields.contains("type") || !obj.fields.contains("data")) {
          deserializationError(s"Unknown 'type' or 'data' of JSON value: ${
            obj.compactPrint
          }")
        }
        val jsData = obj.fields("data")

        obj.fields("type") match {
          case JsString("byte") => asJsType[JsNumber](jsData).value.byteValue()
          case JsString("short") => asJsType[JsNumber](jsData).value.shortValue()
          case JsString("int") => asJsType[JsNumber](jsData).value.intValue()
          case JsString("long") => asJsType[JsNumber](jsData).value.longValue()
          case JsString("float") => asJsType[JsNumber](jsData).value.floatValue()
          case JsString("double") => asJsType[JsNumber](jsData).value.doubleValue()
          case JsString("timestamp") => new Timestamp(asJsType[JsNumber](jsData).value.longValue())
          case JsString("date") => new Date(asJsType[JsNumber](jsData).value.longValue())
          case JsString("array") =>
            val arrObj = asJsType[JsArray](jsData).elements.map(readJson).toArray
            arrObj.headOption match {
              case Some(_: Byte) => arrObj.map[Byte, Array[Byte]](_.asInstanceOf[Byte])
              case Some(_: Short) => arrObj.map[Short, Array[Short]](_.asInstanceOf[Short])
              case Some(_: Int) => arrObj.map[Int, Array[Int]](_.asInstanceOf[Int])
              case Some(_: Long) => arrObj.map[Long, Array[Long]](_.asInstanceOf[Long])
              case Some(_: Float) => arrObj.map[Float, Array[Float]](_.asInstanceOf[Float])
              case Some(_: Double) => arrObj.map[Double, Array[Double]](_.asInstanceOf[Double])
              case Some(_: String) => arrObj.map[String, Array[String]](_.asInstanceOf[String])
              case Some(_: Boolean) => arrObj.map[Boolean, Array[Boolean]](_.asInstanceOf[Boolean])
              case _ => arrObj
            }
            //val arr = Array.newBuilder[Byte]
            //arr ++= asJsType[JsArray](jsData).elements.map {
            //  v => asJsType[JsNumber](v).value.byteValue()
            //}
            //arr.result()
          case JsString("wrapped_array") =>
            mutable.WrappedArray.make(asJsType[JsArray](jsData).elements.map(readJson).toArray)
          case JsString("map") =>
            val elements = asJsType[JsArray](jsData).elements.map {
              case o: JsObject =>
                if (!o.fields.contains("key") || !o.fields.contains("val")) {
                  deserializationError(s"Unknown 'key' or 'val' of JSON value: ${
                    o.compactPrint
                  }")
                }
                readJson(o.fields("key")) -> readJson(o.fields("val"))
              case other =>
                deserializationError(s"Expected a JsObject, got: ${
                  other.getClass.getName
                }")
            }
            Map(elements: _*)
          case JsString("seq") => asJsType[JsArray](jsData).elements.map(readJson)

          case other =>
            deserializationError(s"Expected type as 'array', 'wrapped_array' or 'map', " +
              s"got: ${other.getClass.getName}: ${other.compactPrint}")
        }
      case other =>
        deserializationError(s"Don't support deserializing JSON value: ${other.compactPrint}")
    }
  }
}
