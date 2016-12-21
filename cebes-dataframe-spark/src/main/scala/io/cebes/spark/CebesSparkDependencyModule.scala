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
 * Created by phvu on 06/09/16.
 */

package io.cebes.spark

import com.google.inject.AbstractModule
import com.google.inject.assistedinject.FactoryModuleBuilder
import io.cebes.df.support.{GroupedDataframe, NAFunctions, StatFunctions}
import io.cebes.df.{Dataframe, DataframeStore}
import io.cebes.spark.config.{HasSparkSession, HasSparkSessionProvider}
import io.cebes.spark.df.support.{SparkGroupedDataframe, SparkNAFunctions, SparkStatFunctions}
import io.cebes.spark.df.{DataframeFactory, SparkDataframe, SparkDataframeStore}



class CebesSparkDependencyModule extends AbstractModule {

  protected def configure(): Unit = {

    install(new FactoryModuleBuilder()
      .implement(classOf[Dataframe], classOf[SparkDataframe])
      .implement(classOf[GroupedDataframe], classOf[SparkGroupedDataframe])
      .implement(classOf[NAFunctions], classOf[SparkNAFunctions])
      .implement(classOf[StatFunctions], classOf[SparkStatFunctions])
      .build(classOf[DataframeFactory]))

    //TODO: needs to make DataframeFactory to be singleton

    bind(classOf[HasSparkSession]).toProvider(classOf[HasSparkSessionProvider])
    bind(classOf[DataframeStore]).to(classOf[SparkDataframeStore])
  }
}
