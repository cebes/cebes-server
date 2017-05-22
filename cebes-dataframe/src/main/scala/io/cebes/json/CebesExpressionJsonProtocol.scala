/* Copyright 2017 The Cebes Authors. All Rights Reserved.
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

import io.cebes.df.Column
import io.cebes.df.expressions._
import io.cebes.json.CebesCoreJsonProtocol._
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.reflect.runtime.universe
import scala.util.{Failure, Success, Try}

/**
  * The JsonFormat in this trait should only (de)serialize the fields of the object.
  * No need to write the class name.
  *
  * This contains the generic JsonFormat for [[Expression]].
  * Any specific implementation of cebes-dataframe, that add new sub-types of [[Expression]]
  * should extend this trait and provide a companion object for it.
  */
trait CebesExpressionJsonProtocol extends GenericJsonProtocol {

  /////////////////////////////////////////////////////////////////////////////
  // Expression
  /////////////////////////////////////////////////////////////////////////////

  private lazy val runtimeMirror: universe.Mirror = universe.runtimeMirror(getClass.getClassLoader)

  private lazy val exprJsonProtocolMembers = runtimeMirror.classSymbol(getClass).info.members
  private lazy val exprJsonProtocolMirror: universe.InstanceMirror = runtimeMirror.reflect(this)

  implicit object ExpressionFormat extends JsonFormat[Expression] {


    private def isChildJsonFormat(t: universe.Type, classSymbol: universe.ClassSymbol): Boolean =
      t <:< universe.appliedType(universe.typeOf[JsonFormat[_]], classSymbol.toType)

    /**
      * Find JsonFormat[_] for the given classSymbol, expected to be in [[CebesExpressionJsonProtocol]]
      */
    private def findCustomJsonFormat(classSymbol: universe.ClassSymbol): Option[JsonFormat[Expression]] = {
      exprJsonProtocolMembers.collectFirst {
        case s if s.isModule && isChildJsonFormat(s.info, classSymbol) =>
          exprJsonProtocolMirror.reflectModule(s.asModule).instance.asInstanceOf[JsonFormat[Expression]]
        case s if s.isMethod && isChildJsonFormat(s.asMethod.returnType, classSymbol)
          && s.asMethod.paramLists.isEmpty =>
          exprJsonProtocolMirror.reflectMethod(s.asMethod).apply().asInstanceOf[JsonFormat[Expression]]
      }
    }

    private def writeExpressionGeneric(expr: Expression): JsValue = {

      val classSymbol = runtimeMirror.classSymbol(expr.getClass)

      val jsFields = findCustomJsonFormat(classSymbol) match {
        case Some(jsFormat) =>
          jsFormat.write(expr) match {
            case jsObj: JsObject =>
              jsObj.fields
            case other =>
              serializationError(s"Custom serialization logic must return a JsObject. Got ${other.getClass.getName}")
          }
        case None =>
          val instanceMirror: universe.InstanceMirror = runtimeMirror.reflect(expr)
          if (!classSymbol.isCaseClass || classSymbol.primaryConstructor.asMethod.paramLists.length != 1) {
            serializationError("Generic serializer for Expression only works for " +
              "case class with one parameter list. Please provide custom serialization " +
              s"logic via ExpressionCustomJsonProtocol for ${classSymbol.name.toString}")
          }

          val params = classSymbol.primaryConstructor.asMethod.paramLists.head
          params.map { p =>
            val v = instanceMirror.reflectField(classSymbol.toType.member(p.name).asTerm).get
            val jsVal = v match {
              case subExpr: Expression => write(subExpr)
              case vv: Any =>
                Try(writeJson(vv)) match {
                  case Success(js) => js
                  case Failure(f) =>
                    deserializationError(s"Failed to serialize ${classSymbol.name.toString}. " +
                      s"Please provide custom serialization logic via ExpressionCustomJsonProtocol: ${f.getMessage}")
                }
            }
            p.name.toString -> jsVal
          }.toMap
      }
      JsObject(Map("className" -> JsString(expr.getClass.getName)) ++ jsFields)
    }

    override def write(obj: Expression): JsValue = writeExpressionGeneric(obj)

    ////////////////
    // read
    ////////////////

    private def readExpressionGeneric(json: JsValue): Any = json match {
      case jsObj: JsObject =>
        jsObj.fields.get("className") match {
          case Some(JsString(className)) =>

            val classSymbol: universe.ClassSymbol = runtimeMirror.classSymbol(Class.forName(className))

            findCustomJsonFormat(classSymbol) match {
              case Some(jsFormat) =>
                jsFormat.read(jsObj)

              case None =>
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
            }
          case _ =>
            readJson(jsObj)
        }
      case other =>
        readJson(other)
    }

    override def read(json: JsValue): Expression = readExpressionGeneric(json) match {
      case expr: Expression => expr
      case other => deserializationError(s"Got unwanted type from Json: ${other.toString}")
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

  /////////////////////////////////////////////////////////////////////////////
  // Custom JsonFormat for subclasses of [[Expression]] that aren't serializable
  // by the generic logic above
  /////////////////////////////////////////////////////////////////////////////

  implicit val countDistinctJsonFormat: RootJsonFormat[CountDistinct] = jsonFormat(CountDistinct.apply, "expr", "exprs")
  implicit val groupingIDJsonFormat: RootJsonFormat[GroupingID] = jsonFormat1(GroupingID)

  implicit val castJsonFormat: RootJsonFormat[Cast] = jsonFormat2(Cast)

  implicit val greatestJsonFormat: RootJsonFormat[Greatest] = jsonFormat1(Greatest)
  implicit val leastJsonFormat: RootJsonFormat[Least] = jsonFormat1(Least)

  implicit val createArrayJsonFormat: RootJsonFormat[CreateArray] = jsonFormat1(CreateArray)
  implicit val createMapJsonFormat: RootJsonFormat[CreateMap] = jsonFormat1(CreateMap)
  implicit val createStructJsonFormat: RootJsonFormat[CreateStruct] = jsonFormat1(CreateStruct)

  implicit val caseWhenJsonFormat: RootJsonFormat[CaseWhen] = jsonFormat2(CaseWhen)

  implicit val murmur3HashJsonFormat: RootJsonFormat[Murmur3Hash] = jsonFormat1(Murmur3Hash)

  implicit val coalesceJsonFormat: RootJsonFormat[Coalesce] = jsonFormat1(Coalesce)

  implicit val inJsonFormat: RootJsonFormat[In] = jsonFormat2(In)

  implicit val concatJsonFormat: RootJsonFormat[Concat] = jsonFormat1(Concat)
  implicit val concatWsJsonFormat: RootJsonFormat[ConcatWs] = jsonFormat2(ConcatWs)
  implicit val formatStringJsonFormat: RootJsonFormat[FormatString] = jsonFormat2(FormatString)

}

/**
  * Default object for ExpressionJsonProtocol, intended to be used for test suites in this module only.
  */
private[json] object CebesExpressionDefaultJsonProtocol extends CebesExpressionJsonProtocol
