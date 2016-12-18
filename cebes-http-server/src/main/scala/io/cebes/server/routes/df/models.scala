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
 * Created by phvu on 13/12/2016.
 */

package io.cebes.server.routes.df

import java.util.UUID

import io.cebes.df.sample.DataSample
import io.cebes.df.schema.Schema
import io.cebes.server.routes.HttpJsonProtocol
import spray.json._

import scala.collection.mutable
import scala.reflect.ClassTag

case class SampleRequest(df: UUID, withReplacement: Boolean, fraction: Double, seed: Long)

case class TakeRequest(df: UUID, n: Int)

trait HttpDfJsonProtocol extends HttpJsonProtocol {

  implicit val sampleRequestFormat = jsonFormat4(SampleRequest)
  implicit val takeRequestFormat = jsonFormat2(TakeRequest)

  implicit object DataSampleFormat extends JsonFormat[DataSample] {

    override def write(obj: DataSample): JsValue = {
      val data = obj.schema.fields.zip(obj.data).map {
        case (_, col) => JsArray(col.map(s => writeJson(s)): _*)
      }
      JsObject(Map("schema" -> obj.schema.toJson, "data" -> JsArray(data: _*)))
    }

    private def toJsObject(typeName: String, data: JsValue): JsObject = {
      JsObject(Map("type" -> JsString(typeName), "data" -> data))
    }

    private def asJsType[T <: JsValue](js: JsValue)(implicit tag: ClassTag[T]): T = js match {
      case v: T => v
      case other =>
        deserializationError(s"Expected a ${tag.runtimeClass.asInstanceOf[Class[T]].getName}, " +
          s"got ${other.getClass.getName}")
    }

    private def writeJson(value: Any): JsValue = {
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
        case arr: Array[_] =>
          if (arr.length > 0 && !arr.head.isInstanceOf[Byte]) {
            serializationError(s"Only an array of bytes is supported. " +
              s"Got an array of ${arr.head.getClass.getName}")
          }
          toJsObject("byte_array", JsArray(arr.map { v => JsNumber(v.asInstanceOf[Byte]) }: _*))
        case arr: mutable.WrappedArray[_] =>
          toJsObject("wrapped_array", JsArray(arr.map(writeJson): _*))
        case m: Map[_, _] =>
          val arr = JsArray(m.map {
            case (k, v) => JsObject(Map("key" -> writeJson(k), "val" -> writeJson(v)))
          }.toVector)
          toJsObject("map", arr)
        case other =>
          serializationError(s"Don't know how to serialize values of class ${other.getClass.getName}")
      }
    }

    private def readJson(js: JsValue): Any = {
      js match {
        case JsNull => null
        case JsString(s) => s
        case JsBoolean(v) => v
        case obj: JsObject =>
          if (!obj.fields.contains("type") || !obj.fields.contains("data")) {
            deserializationError(s"Unknown 'type' or 'data' of JSON value: ${obj.compactPrint}")
          }
          val jsData = obj.fields("data")

          obj.fields("type") match {
            case JsString("byte") => asJsType[JsNumber](jsData).value.byteValue()
            case JsString("short") => asJsType[JsNumber](jsData).value.shortValue()
            case JsString("int") => asJsType[JsNumber](jsData).value.intValue()
            case JsString("long") => asJsType[JsNumber](jsData).value.longValue()
            case JsString("float") => asJsType[JsNumber](jsData).value.floatValue()
            case JsString("double") => asJsType[JsNumber](jsData).value.doubleValue()
            case JsString("byte_array") =>
              val arr = Array.newBuilder[Byte]
              arr ++= asJsType[JsArray](jsData).elements.map { v => asJsType[JsNumber](v).value.byteValue() }
              arr.result()
            case JsString("wrapped_array") =>
              mutable.WrappedArray.make(asJsType[JsArray](jsData).elements.map(readJson).toArray)
            case JsString("map") =>
              val elements = asJsType[JsArray](jsData).elements.map {
                case o: JsObject =>
                  if (!o.fields.contains("key") || !o.fields.contains("val")) {
                    deserializationError(s"Unknown 'key' or 'val' of JSON value: ${o.compactPrint}")
                  }
                  readJson(o.fields("key")) -> readJson(o.fields("val"))
                case other =>
                  deserializationError(s"Expected a JsObject, got: ${other.getClass.getName}")
              }
              Map(elements: _*)
            case other =>
              deserializationError(s"Expected type as 'array', 'wrapped_array' or 'map', " +
                s"got: ${other.getClass.getName}: ${other.compactPrint}")
          }
        case other =>
          deserializationError(s"Don't support deserializing JSON value: ${other.compactPrint}")
      }
    }

    override def read(json: JsValue): DataSample = {
      json match {
        case obj: JsObject =>
          require(obj.fields.contains("schema") && obj.fields.contains("data"),
            "Json object of DataSample must have fields named 'schema' and 'data'")

          val schema = obj.fields("schema").convertTo[Schema]
          val data = obj.fields("data") match {
            case arr: JsArray =>
              require(schema.size == arr.elements.length,
                s"Length of schema (${schema.size}) must equal " +
                  s"the number of columns in data (${arr.elements.length})")
              schema.fields.zip(arr.elements).map {
                case (f, col) =>
                  col match {
                    case arrCol: JsArray =>
                      arrCol.elements.map(readJson)
                    case a =>
                      deserializationError(s"Expect a JsArray object, got ${a.getClass.getName} instead")
                  }
              }
            case a => deserializationError(s"Expected data as an Array, got ${a.getClass.getName} instead")
          }
          new DataSample(schema, data)
        case other => deserializationError(s"Expected a JsObject, but got ${other.getClass.getName}")
      }
    }

  }

}

object HttpDfJsonProtocol extends HttpDfJsonProtocol
