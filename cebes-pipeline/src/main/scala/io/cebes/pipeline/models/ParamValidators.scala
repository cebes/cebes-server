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

object ParamValidators {

  /**
    * Default validator that validates anything
    */
  def default[T]: T => Boolean = { _ =>
    true
  }

  /** Only validate if the value satisfies the given regexp */
  def regexp(re: String): String => Boolean = { s =>
    s.matches(re)
  }

  /**
    * Validate if the value belong to the given list of options
    */
  def oneOf[T](options: T*): T => Boolean = options.contains

  /** Validate x if x >= min */
  def greaterOrEqual(min: Double): Double => Boolean = { v =>
    v >= min
  }

  /////////////////////////////////////////////////////////////////////////////
  // Specialized validators
  /////////////////////////////////////////////////////////////////////////////

  private lazy val paramNamePattern = new Regex("[a-z][a-zA-Z0-9_]*").pattern

  /** Check if the given string is a valid name for a [[Param]] */
  def checkValidParamName(name: String): Unit = {
    require(paramNamePattern.matcher(name).matches(),
      s"Invalid param name: $name. Must only contain a-z, A-Z, 0-9, _ and starts with a-z")
  }

  private lazy val stageNamePattern: Pattern = new Regex("[a-z][a-z0-9_./]*").pattern

  /** If the given string is a valid name for a stage */
  def isValidStageName(name: String): Boolean = stageNamePattern.matcher(name).matches()

  /** Regex that validate a slot descriptor */
  lazy val slotDescriptorRegex: Regex = new Regex("([a-z][a-z0-9_./]*)(:([0-9]+))?", "parent", "", "slot")
}
