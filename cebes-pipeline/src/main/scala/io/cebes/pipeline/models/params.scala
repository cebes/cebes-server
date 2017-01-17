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

import java.lang.reflect.Modifier

import io.cebes.df.Column

import scala.collection.mutable

/**
  * Generic parameter with default value, documentation and some validator
  */
class Param[T](val name: String, val defaultValue: Option[T],
               val doc: String, val validator: T => Boolean = ParamValidators.default) {
  ParamValidators.checkValidParamName(name)
  if (defaultValue.isDefined) {
    require(validator(defaultValue.get),
      s"$toString: Invalid default value (${defaultValue.get.toString}) for parameter $name")
  }
}

case class StringParam(override val name: String, override val defaultValue: Option[String],
                       override val doc: String,
                       override val validator: String => Boolean = ParamValidators.default)
  extends Param[String](name, defaultValue, doc, validator)

case class BooleanParam(override val name: String, override val defaultValue: Option[Boolean],
                        override val doc: String,
                        override val validator: Boolean => Boolean = ParamValidators.default)
  extends Param[Boolean](name, defaultValue, doc, validator)

case class IntParam(override val name: String, override val defaultValue: Option[Int],
                    override val doc: String,
                    override val validator: Int => Boolean = ParamValidators.default)
  extends Param[Int](name, defaultValue, doc, validator)

case class LongParam(override val name: String, override val defaultValue: Option[Long],
                       override val doc: String,
                       override val validator: Long => Boolean = ParamValidators.default)
  extends Param[Long](name, defaultValue, doc, validator)

case class FloatParam(override val name: String, override val defaultValue: Option[Float],
                      override val doc: String,
                      override val validator: Float => Boolean = ParamValidators.default)
  extends Param[Float](name, defaultValue, doc, validator)

case class DoubleParam(override val name: String, override val defaultValue: Option[Double],
                       override val doc: String,
                       override val validator: Double => Boolean = ParamValidators.default)
  extends Param[Double](name, defaultValue, doc, validator)

case class StringArrayParam(override val name: String, override val defaultValue: Option[Array[String]],
                            override val doc: String,
                            override val validator: Array[String] => Boolean = ParamValidators.default)
  extends Param[Array[String]](name, defaultValue, doc, validator)

case class ColumnParam(override val name: String, override val defaultValue: Option[Column],
                       override val doc: String,
                       override val validator: Column => Boolean = ParamValidators.default)
  extends Param[Column](name, defaultValue, doc, validator)

/**
  * Trait for components that take parameters. This also provides an internal param map to store
  * parameter values attached to the instance.
  */
trait Params extends Serializable {

  /**
    * Returns all params sorted by their names. The default implementation uses Java reflection to
    * list all public methods that have no arguments and return [[Param]].
    *
    */
  protected lazy val _params: Array[Param[_]] = {
    val methods = this.getClass.getMethods
    methods.filter { m =>
      Modifier.isPublic(m.getModifiers) &&
        classOf[Param[_]].isAssignableFrom(m.getReturnType) &&
        m.getParameterTypes.isEmpty
    }.sortBy(_.getName)
      .map(m => m.invoke(this).asInstanceOf[Param[_]])
  }

  /** Tests whether this instance contains a param with a given name. */
  def hasParam(paramName: String): Boolean = {
    _params.exists(_.name == paramName)
  }

  /** Gets a param by its name. */
  def getParam(paramName: String): Param[Any] = {
    _params.find(_.name == paramName).getOrElse {
      throw new NoSuchElementException(s"Param $paramName does not exist.")
    }.asInstanceOf[Param[Any]]
  }

  /**
    * Sets a parameter in the embedded param map.
    */
  final def set[T](param: Param[T], value: T): this.type = {
    require(param.validator(value),
      s"$toString: Invalid value (${value.toString}) for parameter ${param.name}")
    paramMap.put(param, value)
    this
  }

  /**
    * Sets a parameter (by name) in the embedded param map.
    */
  protected final def set(param: String, value: Any): this.type = {
    set(getParam(param), value)
  }

  /**
    * Optionally returns the user-supplied value of a param.
    */
  final def get[T](param: Param[T]): Option[T] = {
    shouldOwn(param)
    paramMap.get(param)
  }

  /** Take the value of the given parameter */
  final def param[T](p: Param[T]): T = get(p) match {
    case Some(t) => t
    case None => throw new IllegalArgumentException(s"Stage $toString: Parameter ${p.name} is not specified")
  }

  /** Internal param map for user-supplied values. */
  protected final val paramMap: ParamMap = ParamMap.empty

  /** Validates that the input param belongs to this instance. */
  private def shouldOwn(param: Param[_]): Unit = {
    require(hasParam(param.name), s"Param $param does not belong to $this.")
  }

}

///////////////////////////////////////////////////////////////////////////////
// ParamMap
///////////////////////////////////////////////////////////////////////////////

/**
  * A Map of parameters
  */
class ParamMap(private val map: mutable.Map[Param[Any], Any]) {

  def this() = this(mutable.Map.empty)

  /**
    * Puts a (param, value) pair (overwrites if the input param exists).
    */
  def put[T](param: Param[T], value: T): this.type = {
    map(param.asInstanceOf[Param[Any]]) = value
    this
  }

  /**
    * Optionally returns the value associated with a param.
    */
  def get[T](param: Param[T]): Option[T] = {
    map.get(param.asInstanceOf[Param[Any]]).asInstanceOf[Option[T]].orElse(param.defaultValue)
  }

  /**
    * Returns the value associated with a param or a default value.
    */
  def getOrElse[T](param: Param[T], default: T): T = {
    get(param).getOrElse(default)
  }

  /**
    * Gets the value of the input param or its default value if it does not exist.
    * Raises a NoSuchElementException if there is no value associated with the input param.
    */
  def apply[T](param: Param[T]): T = {
    get(param).getOrElse {
      throw new NoSuchElementException(s"Cannot find param ${param.name}.")
    }
  }
}

object ParamMap {

  /**
    * Returns an empty param map.
    */
  def empty: ParamMap = new ParamMap()

}