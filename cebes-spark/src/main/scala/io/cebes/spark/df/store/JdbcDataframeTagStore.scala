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
 * Created by phvu on 29/12/2016.
 */

package io.cebes.spark.df.store

import com.google.inject.{Inject, Singleton}
import io.cebes.df.Dataframe
import io.cebes.persistence.jdbc.TableNames
import io.cebes.persistence.store.JdbcTagStore
import io.cebes.prop.types.MySqlBackendCredentials

/**
  * An implementation of [[io.cebes.store.TagStore[Dataframe]] for Spark Dataframes,
  * with JDBC persistence backend.
  */
@Singleton class JdbcDataframeTagStore @Inject()
(mySqlCreds: MySqlBackendCredentials) extends JdbcTagStore[Dataframe](mySqlCreds, TableNames.DF_TAG_STORE)
