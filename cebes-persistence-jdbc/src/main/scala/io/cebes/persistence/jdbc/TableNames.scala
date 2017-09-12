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
 * Created by phvu on 30/11/2016.
 */

package io.cebes.persistence.jdbc

/**
  * Names of all MySQL tables used in the MySQL backend of cebes-server
  */
object TableNames {

  val REFRESH_TOKENS = "persistence_refresh_tokens"

  val RESULT_STORE = "persistence_result_store"

  val DF_STORE = "dataframe_store"
  val DF_TAG_STORE = "df_tag_store"

  val PIPELINE_STORE = "pipeline_store"
  val PIPELINE_TAG_STORE = "pipeline_tag_store"

  val MODEL_STORE = "model_store"
  val MODEL_TAG_STORE = "model_tag_store"

  val REPOSITORY_REFRESH_TOKENS = "repo_refresh_tokens"
}
