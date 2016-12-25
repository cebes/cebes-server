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

import java.sql.Timestamp
import java.util.Date

import io.cebes.df.expressions.Expression
import io.cebes.df.sample.DataSample
import io.cebes.df.schema.{Schema, SchemaField}
import io.cebes.df.types.VariableTypes.VariableType
import io.cebes.df.types.storage._
import io.cebes.df.types.{StorageTypes, VariableTypes}
import io.cebes.storage.DataFormats
import io.cebes.storage.DataFormats.DataFormat
import spray.json._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.util.{Failure, Success, Try}

/**
  * All JSON protocols for all JSON-serializable classes in cebes-dataframe
  */
trait CebesCoreJsonProtocol extends DefaultJsonProtocol {

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

  abstract class AbstractExpressionFormat extends JsonFormat[Expression] {

    protected def writeExpression(expr: Expression): Option[JsValue]

    private def writeExpressionGeneric(expr: Expression): JsValue = {
      val classSymbol = runtimeMirror.classSymbol(expr.getClass)
      val instanceMirror: universe.InstanceMirror = runtimeMirror.reflect(expr)

      if (!classSymbol.isCaseClass || classSymbol.primaryConstructor.asMethod.paramLists.length != 1) {
        serializationError("Generic serializer for Expression only works for " +
          "case class with one parameter list. Please provide custom serialization " +
          s"logic in writeExpression() for ${expr.getClass.getName}")
      }

      val params = classSymbol.primaryConstructor.asMethod.paramLists.head

      val jsParams = params.zipWithIndex.map { case (p, idx) =>
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
            JsObject(Map("exprType" -> JsString("Option[Expression]"),
              "value" -> JsNull))

          case vv: Any => writeJson(vv)
        }
        s"param_$idx" -> jsVal
      }.toMap

      JsObject(Map("exprType" -> JsString(expr.getClass.getName)) ++ jsParams)
    }

    override def write(obj: Expression): JsValue =
      writeExpression(obj).getOrElse(writeExpressionGeneric(obj))

    ////////////////
    // read
    ////////////////

    protected def readExpression(json: JsValue): Option[Expression]

    private def readExpressionGeneric(json: JsValue): Any = {
      json match {
        case jsArr: JsArray => jsArr.elements.map {
          js => readExpression(js).getOrElse(readExpressionGeneric(js))
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

            case Some(JsString(className)) =>

              @tailrec
              def getField(idx: Int, result: Seq[Any]): Seq[Any] = {
                jsObj.fields.get(s"param_$idx") match {
                  case None => result
                  case Some(js) =>
                    getField(idx + 1, result :+ readExpression(js).getOrElse(readExpressionGeneric(js)))
                }
              }

              val params = getField(0, Seq.empty[Any])

              val classSymbol: universe.ClassSymbol = runtimeMirror.classSymbol(Class.forName(className))
              val classMirror: universe.ClassMirror = runtimeMirror.reflectClass(classSymbol)

              val constructorSymbol = classSymbol.primaryConstructor.asMethod
              if (!classSymbol.isCaseClass || constructorSymbol.paramLists.length != 1) {
                deserializationError("Generic serializer for Expression only works for " +
                  "case class with one parameter list. Please provide custom deserialization " +
                  s"logic in readExpression() for $className")
              }
              if (constructorSymbol.paramLists.head.length != params.length) {
                deserializationError(s"The constructor of the loaded class (${classSymbol.name}) " +
                  s"requires ${constructorSymbol.paramLists.head.length} parameters, while we got " +
                  s"${params.length} from JSON")
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

    override def read(json: JsValue): Expression =
      readExpression(json).getOrElse {
        readExpressionGeneric(json) match {
          case expr: Expression => expr
          case other => deserializationError(s"Got unwanted type from Json: ${other.toString}")
        }
      }
  }

  /////////////////////////////////////////////////////////////////////////////
  // General helpers, for dealing with basic types
  /////////////////////////////////////////////////////////////////////////////

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
        if (arr.length > 0 && !arr.head.isInstanceOf[Byte]) {
          serializationError(s"Only an array of bytes is supported. " +
            s"Got an array of ${
              arr.head.getClass.getName
            }")
        }
        toJsObject("byte_array", JsArray(arr.map {
          v => JsNumber(v.asInstanceOf[Byte])
        }: _*))
      case arr: mutable.WrappedArray[_] =>
        toJsObject("wrapped_array", JsArray(arr.map(writeJson): _*))
      case m: Map[_, _] =>
        val arr = JsArray(m.map {
          case (k, v) => JsObject(Map("key" -> writeJson(k), "val" -> writeJson(v)))
        }.toVector)
        toJsObject("map", arr)
      case seq: Seq[_] =>
        toJsObject("seq", JsArray(seq.map(writeJson): _*))
      case s: StorageType =>
        toJsObject("storageType", s.toJson)
      case other =>
        serializationError(s"Don't know how to serialize values of class ${
          other.getClass.getName
        }")
    }
  }

  private def asJsType[T <: JsValue](js: JsValue)(implicit tag: ClassTag[T]): T = js match {
    case v: T => v
    case other =>
      deserializationError(s"Expected a ${
        tag.runtimeClass.asInstanceOf[Class[T]].getName
      }, " +
        s"got ${
          other.getClass.getName
        }")
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
          case JsString("byte_array") =>
            val arr = Array.newBuilder[Byte]
            arr ++= asJsType[JsArray](jsData).elements.map {
              v => asJsType[JsNumber](v).value.byteValue()
            }
            arr.result()
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
          case JsString("storageType") => jsData.convertTo[StorageType]
          case other =>
            deserializationError(s"Expected type as 'array', 'wrapped_array' or 'map', " +
              s"got: ${
                other.getClass.getName
              }: ${
                other.compactPrint
              }")
        }
      case other =>
        deserializationError(s"Don't support deserializing JSON value: ${
          other.compactPrint
        }")
    }
  }

  protected def writeMap[K <: Any, V <: Any](m: Map[K, V]): JsValue = {
    val jsValues = m.map {
      case (key, value) =>
        JsArray(writeJson(key), writeJson(value))
    }.toSeq
    JsArray(jsValues: _*)
  }

  protected def readMap[K, V](json: JsValue)(implicit tagK: ClassTag[K], tagV: ClassTag[V]): Map[K, V] = json match {
    case arr: JsArray =>
      arr.elements.map {
        case element: JsArray =>
          require(element.elements.length == 2, "Require a JsArray of length 2, for the key and value")
          safeCast[K](readJson(element.elements.head)) -> safeCast[V](readJson(element.elements.last))
        case other =>
          deserializationError(s"Expected a JsArray, got ${other.compactPrint}")
      }.toMap
    case other =>
      deserializationError(s"Expected a JsArray, got ${other.compactPrint}")
  }

  protected def safeCast[T](v: Any)(implicit tag: ClassTag[T]): T = {
    v match {
      case null =>
        // some types won't take nulls (e.g. Double)
        // but some types do (e.g. String). So we do whatever we can here.
        tag.runtimeClass.asInstanceOf[Class[T]].cast(null)
      case t =>
        // god helps us
        t.asInstanceOf[T]
    }
  }
}

object CebesCoreJsonProtocol extends CebesCoreJsonProtocol
