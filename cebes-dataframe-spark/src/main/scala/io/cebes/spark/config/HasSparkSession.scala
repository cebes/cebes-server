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

import com.google.inject.{Inject, Provider, Singleton}
import io.cebes.prop.{Prop, Property}
import org.apache.spark.sql.SparkSession

trait HasSparkSession {

  val session: SparkSession
}

class HasSparkSessionProvider @Inject()
(@Prop(Property.SPARK_MODE) val sparkMode: String) extends Provider[HasSparkSession] {

  override def get(): HasSparkSession = {
    sparkMode.toLowerCase match {
      case "local" => new HasSparkSessionLocal()
      case "yarn" => new HasSparkSessionYarn()
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
