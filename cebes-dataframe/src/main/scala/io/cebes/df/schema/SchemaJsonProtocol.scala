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

package io.cebes.df.schema

import io.cebes.df.types.VariableTypes.VariableType
import io.cebes.df.types.storage._
import io.cebes.df.types.{StorageTypes, VariableTypes}
import spray.json._

import scala.util.{Failure, Success, Try}

trait SchemaJsonProtocol extends MetadataJsonProtocol {

  implicit object StorageTypeFormat extends JsonFormat[StorageType] {

    def write(obj: StorageType): JsValue = {
      if (StorageTypes.atomicTypes.contains(obj)) {
        JsString(obj.typeName)
      } else {
        obj match {
          case t: ArrayType => t.toJson
          case t: MapType => t.toJson
          case t: StructType => t.toJson
          case _ => serializationError(s"Unknown storage type: ${obj.typeName}")
        }
      }
    }

    def read(json: JsValue): StorageType = json match {
      case JsString(x) => Try(StorageTypes.fromString(x)) match {
        case Success(t) => t
        case Failure(f) =>
          deserializationError(s"Failed to deserialize ${json.compactPrint}: ${f.getMessage}", f)
      }
      case _ =>
        Try(json.convertTo[ArrayType])
          .orElse(Try(json.convertTo[MapType]))
          .orElse(Try(json.convertTo[StructType])) match {
          case Success(t) => t
          case Failure(f) =>
            deserializationError(s"Failed to deserialize ${json.compactPrint}: ${f.getMessage}", f)
        }
    }
  }

  implicit val arrayTypeFormat = jsonFormat1(ArrayType)
  implicit val mapTypeFormat = jsonFormat2(MapType)
  implicit val structFieldFormat = jsonFormat3(StructField)
  implicit val structTypeFormat = jsonFormat1(StructType)

  implicit object VariableTypeFormat extends JsonFormat[VariableType] {

    override def write(obj: VariableType): JsValue = JsString(obj.name)

    override def read(json: JsValue): VariableType = json match {
      case JsString(typeName) => VariableTypes.fromString(typeName) match {
        case Some(t) => t
        case None => deserializationError(s"Unrecognized variable type: $typeName")
      }
      case _ => deserializationError(s"Expected VariableType as a string")
    }

  }

  implicit val schemaFieldFormat = jsonFormat3(SchemaField)
  implicit val schemaFormat = jsonFormat1(Schema)
}

object SchemaJsonProtocol extends SchemaJsonProtocol
