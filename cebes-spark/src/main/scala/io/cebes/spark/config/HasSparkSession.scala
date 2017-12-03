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

import java.util.Properties

import com.google.inject.{Inject, Injector, Provider, Singleton}
import io.cebes.prop.types.HiveMetastoreCredentials
import io.cebes.prop.{Prop, Property}
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.sql.SparkSession

trait HasSparkSession {

  val session: SparkSession
}

/**
  * Provider that provides an instance of [[HasSparkSession]] based on user configuration.
  */
class HasSparkSessionProvider @Inject()
(@Prop(Property.SPARK_MODE) val sparkMode: String,
 val injector: Injector) extends Provider[HasSparkSession] {

  override def get(): HasSparkSession = {
    sparkMode.toLowerCase match {
      case "local" => injector.getInstance(classOf[HasSparkSessionLocal])
      case "yarn" => injector.getInstance(classOf[HasSparkSessionYarn])
      case _ => throw new IllegalArgumentException(s"Invalid spark mode: $sparkMode")
    }
  }
}

////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////

@Singleton class HasSparkSessionLocal @Inject()
(hiveCreds: HiveMetastoreCredentials,
 @Prop(Property.SPARK_WAREHOUSE_DIR) sparkWarehouseDir: String) extends HasSparkSession {

  lazy val session: SparkSession = localSessionBuilder.getOrCreate()

  lazy val localSessionBuilder: SparkSession.Builder = {
    val builder = SparkSession.builder()
      .appName("Cebes service on Spark (local)")
      .master("local[4]")
      .config("spark.sql.warehouse.dir", sparkWarehouseDir)

    // update log4j configuration if there is some log4j.properties in the classpath
    Option(getClass.getClassLoader.getResourceAsStream("log4j.properties")).foreach { f =>
      val props = new Properties()
      props.load(f)
      PropertyConfigurator.configure(props)
    }

    if (hiveCreds.hasJdbcCredentials) {
      builder.config("javax.jdo.option.ConnectionURL", hiveCreds.url)
        .config("javax.jdo.option.ConnectionDriverName", hiveCreds.driver)
        .config("javax.jdo.option.ConnectionUserName", hiveCreds.userName)
        .config("javax.jdo.option.ConnectionPassword", hiveCreds.password)
        .enableHiveSupport()
    } else {
      builder.enableHiveSupport()
    }
  }
}

@Singleton class HasSparkSessionYarn extends HasSparkSession {

  lazy val session: SparkSession = SparkSession.builder()
    .appName("Cebes service on Spark (YARN)")
    .enableHiveSupport()
    .master("yarn").getOrCreate()
}
