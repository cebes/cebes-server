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
 * Created by phvu on 24/08/16.
 */

package io.cebes.server.http

import com.typesafe.config.{Config, ConfigFactory}
import io.cebes.spark.config.CebesSparkConfig

trait CebesHttpConfig extends CebesSparkConfig {

  private val config = ConfigFactory.load()
  private val httpConfig = config.getConfig("http")

  val httpInterface = httpConfig.getString("interface")
  val httpPort = httpConfig.getInt("port")
}

object CebesHttpConfig {

  private def getVariable(systemVar: String,
                          config: Config,
                          configPath: String,
                          defaultValue: String,
                          validOptions: Option[Seq[String]]): String = {

    val v = Option(System.getenv(systemVar)).getOrElse(
      if (config.hasPath(configPath)) config.getString(configPath) else defaultValue)

    validOptions match {
      case Some(options) =>
        options.find(_.equalsIgnoreCase(v)) match {
          case Some(opt) => opt
          case None => throw new IllegalArgumentException(
            s"Invalid value ($v) for variable $systemVar (or $configPath). " +
              s"Valid values are ${options.mkString(", ")}")
        }
      case None => v
    }
  }
}
