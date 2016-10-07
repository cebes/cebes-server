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
 * Created by phvu on 26/09/16.
 */

package io.cebes.df.schema

class Column(val name: String, val storageType: StorageTypes.StorageType,
             @volatile var variableType: VariableTypes.VariableType) {

  /**
    * Construct a column based on the name and the storage type
    * Variable type will be inferred automatically.
    *
    * @param name        name of the column
    * @param storageType storage type of the column
    */
  def this(name: String, storageType: StorageTypes.StorageType) = {
    this(name, storageType, VariableTypes.fromStorageType(storageType))
  }

  def copy(): Column = new Column(this.name, this.storageType, this.variableType)
}
