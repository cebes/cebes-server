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
 * Created by phvu on 18/12/2016.
 */

package io.cebes.json

import java.util.UUID

import io.cebes.df.Column
import io.cebes.df.expressions.Expression
import io.cebes.df.sample.DataSample
import io.cebes.df.schema.{Schema, SchemaField}
import io.cebes.df.types.VariableTypes.VariableType
import io.cebes.df.types.storage._
import io.cebes.df.types.{StorageTypes, VariableTypes}
import io.cebes.storage.DataFormats
import io.cebes.storage.DataFormats.DataFormat
import spray.json._

import scala.reflect.runtime.universe
import scala.util.{Failure, Success, Try}

/**
  * All JSON protocols for all JSON-serializable classes in cebes-dataframe
  */
trait CebesCoreJsonProtocol extends DefaultJsonProtocol with GenericJsonProtocol {

  implicit object UUIDFormat extends JsonFormat[UUID] {

    def write(obj: UUID): JsValue = JsString(obj.toString)

    def read(json: JsValue): UUID = json match {
      case JsString(x) => UUID.fromString(x)
      case _ => deserializationError("Expected UUID as JsString")
    }
  }

  implicit object DataFormatFormat extends JsonFormat[DataFormat] {
    override def write(obj: DataFormat): JsValue = JsString(obj.name)

    override def read(json: JsValue): DataFormat = json match {
      case JsString(fmtName) => DataFormats.fromString(fmtName) match {
        case Some(fmt) => fmt
        case None => deserializationError(s"Unrecognized Data format: $fmtName")
      }
      case _ => deserializationError(s"Expected DataFormat as a string")
    }
  }

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
      val fields = obj.getMap.map {
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

  /////////////////////////////////////////////////////////////////////////////
  // Storage types
  /////////////////////////////////////////////////////////////////////////////

  implicit object StorageTypeFormat extends JsonFormat[StorageType] {

    def write(obj: StorageType): JsValue = {
      obj match {
        case StorageTypes.VectorType =>
          JsString(StorageTypes.VectorType.typeName)
        case t if StorageTypes.atomicTypes.contains(t) =>
          JsString(obj.typeName)
        case t: ArrayType => t.toJson
        case t: MapType => t.toJson
        case t: StructType => t.toJson
        case _ => serializationError(s"Unknown storage type: ${obj.typeName}")
      }
    }

    def read(json: JsValue): StorageType = json match {
      case JsString(x) =>
        Try(StorageTypes.fromString(x)) match {
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

  /////////////////////////////////////////////////////////////////////////////
  // Variable types
  /////////////////////////////////////////////////////////////////////////////

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

  /////////////////////////////////////////////////////////////////////////////
  // Schema
  /////////////////////////////////////////////////////////////////////////////

  implicit val schemaFieldFormat = jsonFormat3(SchemaField)
  implicit val schemaFormat = jsonFormat1(Schema)

  /////////////////////////////////////////////////////////////////////////////
  // DataSample
  /////////////////////////////////////////////////////////////////////////////

  implicit object DataSampleFormat extends JsonFormat[DataSample] {

    override def write(obj: DataSample): JsValue = {
      val data = obj.schema.fields.zip(obj.data).map {
        case (_, col) => JsArray(col.map(s => writeJson(s)): _*)
      }
      JsObject(Map("schema" -> obj.schema.toJson, "data" -> JsArray(data: _*)))
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
                case (_, col) =>
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

  /////////////////////////////////////////////////////////////////////////////
  // Expression
  /////////////////////////////////////////////////////////////////////////////

  private lazy val runtimeMirror: universe.Mirror = universe.runtimeMirror(getClass.getClassLoader)

  implicit object ExpressionFormat extends JsonFormat[Expression] {

    private def writeExpressionGeneric(expr: Expression): JsValue = {
      val classSymbol = runtimeMirror.classSymbol(expr.getClass)
      val instanceMirror: universe.InstanceMirror = runtimeMirror.reflect(expr)

      if (!classSymbol.isCaseClass || classSymbol.primaryConstructor.asMethod.paramLists.length != 1) {
        serializationError("Generic serializer for Expression only works for " +
          "case class with one parameter list. Please provide custom serialization " +
          s"logic in writeExpression() for ${expr.getClass.getName}")
      }

      val params = classSymbol.primaryConstructor.asMethod.paramLists.head

      val jsParams = params.map { p =>
        val v = instanceMirror.reflectField(classSymbol.toType.member(p.name).asTerm).get
        val jsVal = v match {
          case subExpr: Expression => write(subExpr)

          case expressions: Seq[_] if expressions.head.isInstanceOf[Expression] =>
            JsArray(expressions.map(e => write(e.asInstanceOf[Expression])): _*)

          case arr: Seq[_] if arr.head.isInstanceOf[(_, _)]
            && arr.head.asInstanceOf[(_, _)]._1.isInstanceOf[Expression]
            && arr.head.asInstanceOf[(_, _)]._2.isInstanceOf[Expression] =>
            // special case of CaseWhen
            val entries = arr.map(_.asInstanceOf[(Expression, Expression)]).map { tp =>
              JsArray(write(tp._1), write(tp._2))
            }
            JsObject(Map("exprType" -> JsString("Array[Tuple2(Expression)]"),
              "entries" -> JsArray(entries: _*)))

          case opt: Option[_] if opt.nonEmpty && opt.get.isInstanceOf[Expression] =>
            JsObject(Map("exprType" -> JsString("Option[Expression]"),
              "value" -> write(opt.get.asInstanceOf[Expression])))

          case None =>
            JsObject(Map("exprType" -> JsString("Option[Expression]"), "value" -> JsNull))

          case storageType: StorageType =>
            JsObject(Map("exprType" -> JsString("StorageType"), "value" -> storageType.toJson))

          case uuid: UUID =>
            JsObject(Map("exprType" -> JsString("UUID"), "value" -> uuid.toJson))

          case vv: Any => writeJson(vv)
        }
        p.name.toString -> jsVal
      }.toMap

      JsObject(Map("exprType" -> JsString(expr.getClass.getName)) ++ jsParams)
    }

    override def write(obj: Expression): JsValue = writeExpressionGeneric(obj)

    ////////////////
    // read
    ////////////////

    private def readExpressionGeneric(json: JsValue): Any = {
      json match {
        case jsArr: JsArray => jsArr.elements.map {
          js => readExpressionGeneric(js)
        }
        case jsObj: JsObject =>
          jsObj.fields.get("exprType") match {
            case Some(JsString("Array[Tuple2(Expression)]")) =>
              asJsType[JsArray](jsObj.fields("entries")).elements.map(asJsType[JsArray]).map { entry =>
                if (entry.elements.length != 2) {
                  deserializationError(s"Expected list of 2 elements, got ${entry.compactPrint}")
                }
                (read(entry.elements.head), read(entry.elements.last))
              }

            case Some(JsString("Option[Expression]")) =>
              jsObj.fields("value") match {
                case JsNull => None
                case v => Some(read(v))
              }

            case Some(JsString("StorageType")) => jsObj.fields("value").convertTo[StorageType]

            case Some(JsString("UUID")) => jsObj.fields("value").convertTo[UUID]

            case Some(JsString(className)) =>

              val classSymbol: universe.ClassSymbol = runtimeMirror.classSymbol(Class.forName(className))
              val classMirror: universe.ClassMirror = runtimeMirror.reflectClass(classSymbol)

              val constructorSymbol = classSymbol.primaryConstructor.asMethod
              if (!classSymbol.isCaseClass || constructorSymbol.paramLists.length != 1) {
                deserializationError("Generic serializer for Expression only works for " +
                  "case class with one parameter list. Please provide custom deserialization " +
                  s"logic in readExpression() for $className")
              }
              val params = constructorSymbol.paramLists.head.map { p =>
                jsObj.fields.get(p.name.toString) match {
                  case None =>
                    deserializationError(s"Could not find value for parameter ${p.name.toString} of $className")
                  case Some(js) => readExpressionGeneric(js)
                }
              }
              val constructorMirror = classMirror.reflectConstructor(constructorSymbol)
              Try(constructorMirror.apply(params: _*)) match {
                case Success(v) =>
                  v match {
                    case expr: Expression => expr
                    case _ => deserializationError(s"Unknown return type from constructor: ${v.toString}")
                  }

                case Failure(f) => deserializationError("Failed to run constructor", f)
              }

            case _ =>
              readJson(jsObj)
          }
        case other =>
          readJson(other)
      }
    }

    override def read(json: JsValue): Expression = {
      readExpressionGeneric(json) match {
        case expr: Expression => expr
        case other => deserializationError(s"Got unwanted type from Json: ${other.toString}")
      }
    }
  }

  implicit object ColumnFormat extends JsonFormat[Column] {

    override def write(obj: Column): JsValue = {
      JsObject(Map("expr" -> obj.expr.toJson))
    }

    override def read(json: JsValue): Column = json match {
      case jsObj: JsObject =>
        jsObj.fields.get("expr") match {
          case Some(obj) => new Column(obj.convertTo[Expression])
          case _ => deserializationError("Expression must be provided in key 'expr'")
        }
      case _ =>
        deserializationError("A JsObject is expected")
    }
  }


}

object CebesCoreJsonProtocol extends CebesCoreJsonProtocol
