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
 * Created by phvu on 26/08/16.
 */

package io.cebes.storage

import io.cebes.df.Dataframe

trait StorageService {

  /**
    * Write the given dataframe to the given datasource
    *
    * This is an internal API, designed mainly for cebes servers, not for end-users.
    *
    * @param dataframe  data frame to be written
    * @param dataSource data storage to store the given data frame
    */
  def write(dataframe: Dataframe, dataSource: DataSource): Unit

  /**
    * Read the given data source
    *
    * This is end-user API.
    *
    * @param dataSource source to read data from
    * @param options    : additional options to be used while reading the datasource
    * @return a new Dataframe
    */
  def read(dataSource: DataSource, options: Map[String, String]): Dataframe
}
