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

package io.cebes.pipeline.json

import com.google.common.reflect.ClassPath
import com.google.protobuf.ByteString
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType
import com.google.protobuf.Descriptors.{EnumValueDescriptor, FieldDescriptor}
import com.trueaccord.scalapb.{GeneratedMessage, GeneratedMessageCompanion}
import spray.json._

import scala.collection.JavaConversions
import scala.reflect.runtime.universe

trait PipelineJsonProtocol extends DefaultJsonProtocol {

  implicit object ProtobufMessageFormat extends JsonFormat[GeneratedMessage] {

    private lazy val runtimeMirror: universe.Mirror = universe.runtimeMirror(getClass.getClassLoader)

    private lazy val protoMessageClasses = {
      val classpath = ClassPath.from(getClass.getClassLoader)
      JavaConversions.collectionAsScalaIterable(
        classpath.getTopLevelClassesRecursive("io.cebes.pipeline.protos")).flatMap { c =>
        val cls = c.load()
        cls +: cls.getClasses.toSeq
      }.filter { cls =>
        classOf[GeneratedMessage].isAssignableFrom(cls)
      }.map { cls =>
        cls
      }
    }

    override def write(obj: GeneratedMessage): JsValue = {
      val b = List.newBuilder[JsField]
      obj.getAllFields
      b.sizeHint(obj.companion.descriptor.getFields.size)
      val i = obj.companion.descriptor.getFields.iterator
      while (i.hasNext) {
        val f = i.next()
        if (f.getType != FieldDescriptor.Type.GROUP) {
          val name = f.getJsonName
          obj.getField(f) match {
            case null =>
            case Nil if f.isRepeated =>
            case v =>
              b += new JsField(name, serializeField(f, v))
          }
        }
      }
      JsObject(Map("className" -> JsString(obj.getClass.getSimpleName),
        "obj" -> JsObject(b.result(): _*)))
    }

    override def read(json: JsValue): GeneratedMessage = {
      json match {
        case JsObject(fields) if fields.contains("className") =>
          val className = fields("className").asInstanceOf[JsString].value

          val cls = protoMessageClasses.find(_.getSimpleName == className) match {
            case None => deserializationError(s"Proto message class not found: $className")
            case Some(cl) => cl.asInstanceOf[Class[GeneratedMessage]]
          }
          val classSymbol = runtimeMirror.classSymbol(cls)
          val companionMirror = runtimeMirror.reflectModule(classSymbol.companion.asModule)
          val cmp = companionMirror.instance.asInstanceOf[GeneratedMessageCompanion[_]]

          val objFields = fields("obj").asInstanceOf[JsObject].fields
          val values: Map[String, JsValue] = objFields.map(k => k._1 -> k._2)
          val valueMap: Map[FieldDescriptor, Any] =
            JavaConversions.asScalaBuffer(cmp.descriptor.getFields).filter { fd =>
              values.get(fd.getJsonName).isDefined
            }.map { fd =>
              fd -> parseValue(fd, values(fd.getJsonName), cmp)
            }.toMap
          cmp.fromFieldsMap(valueMap).asInstanceOf[GeneratedMessage]
        case _ =>
          deserializationError(s"Expected an object, found ${json.compactPrint}")
      }
    }

    ////////////////////////////////////////////////////////////////////////////////////
    //
    ////////////////////////////////////////////////////////////////////////////////////

    private def defaultValue(fd: FieldDescriptor): Any = {
      require(fd.isOptional)
      fd.getJavaType match {
        case JavaType.INT => 0
        case JavaType.LONG => 0L
        case JavaType.FLOAT | JavaType.DOUBLE => 0.0
        case JavaType.BOOLEAN => false
        case JavaType.STRING => ""
        case JavaType.BYTE_STRING => ByteString.EMPTY
        case JavaType.ENUM => fd.getEnumType.getValues.get(0)
        case JavaType.MESSAGE => throw new RuntimeException("No default value for message")
      }
    }

    @inline
    private def serializeField(fd: FieldDescriptor, value: Any): JsValue = {
      if (fd.isMapField) {
        JsObject(
          value.asInstanceOf[Seq[GeneratedMessage]].map { v =>
            val keyDescriptor = v.companion.descriptor.findFieldByNumber(1)
            val key = Option(v.getField(keyDescriptor)).getOrElse(
              ProtobufMessageFormat.defaultValue(keyDescriptor)).toString
            val valueDescriptor = v.companion.descriptor.findFieldByNumber(2)
            val value = Option(v.getField(valueDescriptor)).getOrElse(
              ProtobufMessageFormat.defaultValue(valueDescriptor))
            key -> serializeSingleValue(valueDescriptor, value)
          }: _*)
      } else if (fd.isRepeated) {
        JsArray(value.asInstanceOf[Seq[Any]].map(serializeSingleValue(fd, _)): _*)
      } else {
        serializeSingleValue(fd, value)
      }
    }

    @inline
    private def serializeSingleValue(fd: FieldDescriptor, value: Any): JsValue = fd.getJavaType match {
      case JavaType.ENUM => JsString(value.asInstanceOf[EnumValueDescriptor].getName)
      case JavaType.MESSAGE => write(value.asInstanceOf[GeneratedMessage])
      case JavaType.INT => JsNumber(value.asInstanceOf[Int])
      case JavaType.LONG => JsNumber(value.asInstanceOf[Long])
      case JavaType.DOUBLE => JsNumber(value.asInstanceOf[Double])
      case JavaType.FLOAT => JsNumber(value.asInstanceOf[Float])
      case JavaType.BOOLEAN => JsBoolean(value.asInstanceOf[Boolean])
      case JavaType.STRING => JsString(value.asInstanceOf[String])
      case JavaType.BYTE_STRING => value.asInstanceOf[ByteString].toByteArray.toJson
    }

    def parseValue(fd: FieldDescriptor, value: JsValue, cmp: GeneratedMessageCompanion[_]): Any = {
      if (fd.isMapField) {
        value match {
          case JsObject(vals) =>
            val mapEntryCmp = cmp.messageCompanionForField(fd)
            val keyDescriptor = fd.getMessageType.findFieldByNumber(1)
            val valueDescriptor = fd.getMessageType.findFieldByNumber(2)
            vals.map {
              case (key, jValue) =>
                val keyObj = keyDescriptor.getJavaType match {
                  case JavaType.BOOLEAN => java.lang.Boolean.valueOf(key)
                  case JavaType.DOUBLE => java.lang.Double.valueOf(key)
                  case JavaType.FLOAT => java.lang.Float.valueOf(key)
                  case JavaType.INT => java.lang.Integer.valueOf(key)
                  case JavaType.LONG => java.lang.Long.valueOf(key)
                  case JavaType.STRING => key
                  case _ => throw new RuntimeException(s"Unsupported type for key for ${fd.getName}")
                }
                mapEntryCmp.fromFieldsMap(Map(keyDescriptor -> keyObj,
                  valueDescriptor -> parseSingleValue(mapEntryCmp, valueDescriptor, jValue)))
            }
          case _ => deserializationError(
            s"Expected an object for map field ${fd.getJsonName} of ${fd.getContainingType.getName}")
        }
      } else if (fd.isRepeated) {
        value match {
          case JsArray(vals) => vals.map(parseSingleValue(cmp, fd, _))
          case _ => deserializationError(
            s"Expected an array for repeated field ${fd.getJsonName} of ${fd.getContainingType.getName}")
        }
      } else {
        parseSingleValue(cmp, fd, value)
      }
    }

    protected def parseSingleValue(cmp: GeneratedMessageCompanion[_], fd: FieldDescriptor, value: JsValue): Any =
      (fd.getJavaType, value) match {
        case (JavaType.ENUM, JsString(s)) => fd.getEnumType.findValueByName(s)
        case (JavaType.MESSAGE, o: JsValue) =>
          // The asInstanceOf[] is a lie: we actually have a companion of some other message (not A),
          // but this doesn't matter after erasure.
          read(o)
        case (JavaType.INT, JsNumber(x)) => x.intValue
        case (JavaType.INT, JsNull) => 0
        case (JavaType.LONG, JsNumber(x)) => x.longValue()
        case (JavaType.LONG, JsNull) => 0L
        case (JavaType.DOUBLE, JsNumber(x)) => x.doubleValue()
        case (JavaType.DOUBLE, JsNull) => 0.toDouble
        case (JavaType.FLOAT, JsNumber(x)) => x.floatValue()
        case (JavaType.FLOAT, JsNull) => 0.toFloat
        case (JavaType.BOOLEAN, JsBoolean(b)) => b
        case (JavaType.BOOLEAN, JsNull) => false
        case (JavaType.STRING, JsString(s)) => s
        case (JavaType.STRING, JsNull) => ""
        case (JavaType.BYTE_STRING, JsNull) => ByteString.EMPTY
        case (JavaType.BYTE_STRING, v: JsValue) => ByteString.copyFrom(v.convertTo[Array[Byte]])
        case _ => deserializationError(
          s"Unexpected value ($value) for field ${fd.getJsonName} of ${fd.getContainingType.getName}")
      }
  }

}

object PipelineJsonProtocol extends PipelineJsonProtocol
