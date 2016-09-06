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

import com.google.inject.{Provider, Singleton}
import org.apache.spark.sql.SparkSession

trait HasSparkSession {

  val session: SparkSession
}

class HasSparkSessionProvider extends Provider[HasSparkSession] with CebesSparkConfig {

  override def get(): HasSparkSession = {
    sparkMode match {
      case CebesSparkConfig.SPARK_MODE_LOCAL => new HasSparkSessionLocal()
      case CebesSparkConfig.SPARK_MODE_YARN => new HasSparkSessionYarn()
      case _ => throw new IllegalArgumentException(s"Invalid spark mode: $sparkMode")
    }
  }
}

@Singleton class HasSparkSessionLocal extends HasSparkSession {

  lazy val session = SparkSession.builder().
    appName("Cebes service on Spark (local)").
    master("local[4]").getOrCreate()
}

@Singleton class HasSparkSessionYarn extends HasSparkSession {

  lazy val session = SparkSession.builder().
    appName("Cebes service on Spark (YARN)").
    master("yarn").getOrCreate()
}