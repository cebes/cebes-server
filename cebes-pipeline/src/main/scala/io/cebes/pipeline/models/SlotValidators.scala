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

import java.util.regex.Pattern

import scala.util.matching.Regex

trait SlotValidator[+T] {
  def check[U >: T](v: U, additionalErrorMsg: String = ""): Unit
}

case class RequiredSlotValidator[T](condition: T => Boolean, errorMsg: String) extends SlotValidator[T] {

  override def check[U >: T](v: U, additionalErrorMsg: String = ""): Unit = require(condition(v.asInstanceOf[T]),
    if (additionalErrorMsg.isEmpty) errorMsg else s"$additionalErrorMsg: $errorMsg")
}

object SlotValidators {

  /**
    * Default validator that validates anything
    */
  def default[T]: SlotValidator[T] = RequiredSlotValidator[T](_ => true, "")

  /** Only validate if the value satisfies the given regexp */
  def regexp(re: String): String => Boolean = { s =>
    s.matches(re)
  }

  /**
    * Validate if the value belong to the given list of options
    */
  def oneOf[T](options: T*): SlotValidator[T] =
    RequiredSlotValidator(options.contains,
      s"Allowed values are: ${options.map(_.toString).mkString(", ")}")

  /** Validate x if x >= min */
  def greaterOrEqual(min: Double): SlotValidator[Double] =
    RequiredSlotValidator((v: Double) => v >= min, s"must be greater or equal than $min")

  /** Validate x if x >= min */
  def greaterOrEqual(min: Int): SlotValidator[Int] =
    RequiredSlotValidator((v: Int) => v >= min, s"must be greater or equal than $min")

  /////////////////////////////////////////////////////////////////////////////
  // Specialized validators
  /////////////////////////////////////////////////////////////////////////////

  private lazy val paramNamePattern = new Regex("[a-z][a-zA-Z0-9_]*").pattern

  /** Check if the given string is a valid name for a [[Slot]] */
  def checkValidSlotName(name: String): Unit = {
    require(paramNamePattern.matcher(name).matches(),
      s"Invalid param name: $name. Must only contain a-z, A-Z, 0-9, _ and starts with a-z")
  }

  private lazy val stageNamePattern: Pattern = new Regex("[a-z][a-z0-9_./]*").pattern

  /** If the given string is a valid name for a stage */
  def isValidStageName: SlotValidator[String] =
    RequiredSlotValidator[String](name => stageNamePattern.matcher(name).matches(),
    "Stage name must begin with a-z and only contains a-z, 0-9, underscore, dot and slash (/)")

  /** Regex that validate a slot descriptor */
  lazy val slotDescriptorRegex: Regex = new Regex("(^[a-z][a-z0-9_./]*)(:([a-z][a-zA-Z0-9_]*))?$", "parent", "", "slot")
}
