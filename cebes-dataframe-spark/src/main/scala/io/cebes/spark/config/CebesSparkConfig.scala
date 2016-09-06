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
 * Created by phvu on 05/09/16.
 */

package io.cebes.spark.config

trait CebesSparkConfig {

  lazy val sparkMode: String = CebesSparkConfig.getVariable("CEBES_SPARK_MODE",
    CebesSparkConfig.SPARK_MODE_LOCAL,
    Some(Seq(CebesSparkConfig.SPARK_MODE_LOCAL, CebesSparkConfig.SPARK_MODE_YARN)))
}

object CebesSparkConfig {

  val SPARK_MODE_LOCAL = "local"
  val SPARK_MODE_YARN = "yarn"

  private def getVariable(systemVar: String,
                          defaultValue: String,
                          validOptions: Option[Seq[String]]): String = {

    val v = Option(System.getenv(systemVar)).getOrElse(defaultValue)
    validOptions match {
      case Some(options) =>
        options.find(_.equalsIgnoreCase(v)) match {
          case Some(opt) => opt
          case None => throw new IllegalArgumentException(
            s"Invalid value ($v) for variable $systemVar. Valid values are ${options.mkString(", ")}")
        }
      case None => v
    }
  }
}
