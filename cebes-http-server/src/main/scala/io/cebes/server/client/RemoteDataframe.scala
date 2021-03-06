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
 * Created by phvu on 14/12/2016.
 */

package io.cebes.server.client

import java.util.UUID

import io.cebes.df.Column
import io.cebes.df.schema.Schema
import io.cebes.spark.df.expressions.SparkPrimitiveExpression

/**
  * A dummy object for representing the Dataframe on the server.
  * This is only used for the client
  *
  * @param id ID of the Dataframe
  */
case class RemoteDataframe(id: UUID, schema: Schema) {

  /**
    * This is a hack, we should create separated "remote" objects
    * for Columns and so on
    */
  def col(colName: String) = new Column(SparkPrimitiveExpression(id, colName, None))
}
