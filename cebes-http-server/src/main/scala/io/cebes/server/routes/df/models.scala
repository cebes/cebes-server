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
import io.cebes.df.types.StorageTypes
import io.cebes.df.types.storage.{ArrayType, MapType, StructType}
import io.cebes.server.routes.HttpJsonProtocol
import spray.json._

case class SampleRequest(df: UUID, withReplacement: Boolean, fraction: Double, seed: Long)

case class TakeRequest(df: UUID, n: Int)

trait HttpDfJsonProtocol extends HttpJsonProtocol {

  implicit val sampleRequestFormat = jsonFormat4(SampleRequest)
  implicit val takeRequestFormat = jsonFormat2(TakeRequest)

  implicit object DataSampleFormat extends JsonFormat[DataSample] {

    override def write(obj: DataSample): JsValue = {
      def safeJson(v: Any, js: => JsValue): JsValue = if (v == null) JsNull else js

      val data = obj.schema.fields.zip(obj.data).map {
        case (f, col) =>
          val colVals = f.storageType match {
            case StorageTypes.StringType =>
              col.map(s => safeJson(s, JsString(s.toString)))
            case StorageTypes.BinaryType =>
              serializationError(s"Don't know how to serialize binary: $col")
            case StorageTypes.DateType =>
              serializationError(s"Don't know how to serialize date: $col")
            case StorageTypes.TimestampType =>
              serializationError(s"Don't know how to serialize timestamp: $col")
            case StorageTypes.CalendarIntervalType =>
              serializationError(s"Don't know how to serialize calendar interval: $col")
            case StorageTypes.BooleanType =>
              col.map(v => safeJson(v, JsBoolean(v.asInstanceOf[Boolean])))
            case t if Seq(StorageTypes.ByteType, StorageTypes.ShortType,
              StorageTypes.IntegerType, StorageTypes.LongType,
              StorageTypes.FloatType, StorageTypes.DoubleType).contains(t) =>
              col.map(v => safeJson(v, JsNumber(v.asInstanceOf[Number].doubleValue())))
            case _: ArrayType =>
              serializationError(s"Don't know how to serialize array: $col")
            case _: MapType =>
              serializationError(s"Don't know how to serialize map: $col")
            case _: StructType =>
              serializationError(s"Don't know how to serialize struct: $col")
            case t =>
              serializationError(s"Don't support serializing type $t: $col")
          }
          JsArray(colVals: _*)
      }
      JsObject(Map("schema" -> obj.schema.toJson, "data" -> JsArray(data: _*)))
    }

    override def read(json: JsValue): DataSample = {
      def safeRead(js: JsValue, v: => Any): Any = if (js == JsNull) null else v

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
                      f.storageType match {
                        case StorageTypes.StringType =>
                          arrCol.elements.map(s => safeRead(s, s.convertTo[String]))
                        case StorageTypes.BinaryType =>
                          deserializationError(s"Don't know how to deserialize binary: $arrCol")
                        case StorageTypes.DateType =>
                          deserializationError(s"Don't know how to deserialize date: $arrCol")
                        case StorageTypes.TimestampType =>
                          deserializationError(s"Don't know how to deserialize timestamp: $arrCol")
                        case StorageTypes.CalendarIntervalType =>
                          deserializationError(s"Don't know how to deserialize calendar interval: $arrCol")
                        case StorageTypes.BooleanType =>
                          arrCol.elements.map(v => safeRead(v, v.convertTo[Boolean]))
                        case StorageTypes.ByteType =>
                          arrCol.elements.map(v => safeRead(v, v.convertTo[Byte]))
                        case StorageTypes.ShortType =>
                          arrCol.elements.map(v => safeRead(v, v.convertTo[Short]))
                        case StorageTypes.IntegerType =>
                          arrCol.elements.map(v => safeRead(v, v.convertTo[Int]))
                        case StorageTypes.LongType =>
                          arrCol.elements.map(v => safeRead(v, v.convertTo[Long]))
                        case StorageTypes.FloatType =>
                          arrCol.elements.map(v => safeRead(v, v.convertTo[Float]))
                        case StorageTypes.DoubleType =>
                          arrCol.elements.map(v => safeRead(v, v.convertTo[Double]))
                        case _: ArrayType =>
                          deserializationError(s"Don't know how to deserialize array: $arrCol")
                        case _: MapType =>
                          deserializationError(s"Don't know how to deserialize map: $arrCol")
                        case _: StructType =>
                          deserializationError(s"Don't know how to deserialize struct: $arrCol")
                        case t =>
                          deserializationError(s"Don't support deserialize type $t: $arrCol")
                      }
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
