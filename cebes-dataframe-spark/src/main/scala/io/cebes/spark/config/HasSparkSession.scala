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
(@Prop(Property.SPARK_MODE) val sparkMode: String,
 @Prop(Property.HIVE_METASTORE_URL) val hiveMetastoreUrl: String,
 @Prop(Property.HIVE_METASTORE_DRIVER) val hiveMetastoreDriver: String,
 @Prop(Property.HIVE_METASTORE_USERNAME) val hiveMetastoreUserName: String,
 @Prop(Property.HIVE_METASTORE_PASSWORD) val hiveMetastorePwd: String) extends Provider[HasSparkSession] {

  override def get(): HasSparkSession = {
    sparkMode.toLowerCase match {
      case "local" => new HasSparkSessionLocal(hiveMetastoreUrl, hiveMetastoreDriver,
        hiveMetastoreUserName, hiveMetastorePwd)
      case "yarn" => new HasSparkSessionYarn()
      case _ => throw new IllegalArgumentException(s"Invalid spark mode: $sparkMode")
    }
  }
}

@Singleton class HasSparkSessionLocal(hiveMetastoreUrl: String,
                                      hiveMetastoreDriver: String,
                                      hiveMetastoreUser: String,
                                      hiveMetastorePwd: String) extends HasSparkSession {

  lazy val session = getLocalSession.getOrCreate()

  def getLocalSession: SparkSession.Builder = {
    val builder = SparkSession.builder()
      .appName("Cebes service on Spark (local)")
      .master("local[4]")
      .config("spark.sql.warehouse.dir",
        s"file:${System.getProperty("java.io.tmpdir", "/tmp")}/spark-warehouse")

    if (!hiveMetastoreUser.isEmpty && !hiveMetastoreUrl.isEmpty && !hiveMetastorePwd.isEmpty) {
      builder.config("javax.jdo.option.ConnectionURL", hiveMetastoreUrl)
        .config("javax.jdo.option.ConnectionDriverName", hiveMetastoreDriver)
        .config("javax.jdo.option.ConnectionUserName", hiveMetastoreUser)
        .config("javax.jdo.option.ConnectionPassword", hiveMetastorePwd)
        .enableHiveSupport()
    } else {
      builder.enableHiveSupport()
    }
  }
}

@Singleton class HasSparkSessionYarn extends HasSparkSession {

  lazy val session = SparkSession.builder()
    .appName("Cebes service on Spark (YARN)")
    .enableHiveSupport()
    .master("yarn").getOrCreate()
}
