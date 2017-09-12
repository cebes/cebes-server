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
 * Created by phvu on 13/12/2016.
 */

package io.cebes.server.routes.common

import io.cebes.df.Dataframe
import io.cebes.http.server.operations.AsyncOperation

/**
  * Abstract class for *asynchronous* operations that return a Dataframe.
  * Subclasses of this class only need to override the runImpl() function
  *
  * @tparam E Type of the request entity
  */
abstract class AsyncDataframeOperation[E] extends AsyncOperation[E, Dataframe, DataframeResponse] {

  /**
    * Transform the actual result (of type T)
    * into something that will be returned to the clients
    * Normally R should be Json-serializable.
    *
    * @param requestEntity The request entity
    * @param result        The actual result, returned by `runImpl`
    * @return a JSON-serializable object, to be returned to the clients
    */
  override protected def transformResult(requestEntity: E, result: Dataframe): Option[DataframeResponse] = {
    Some(DataframeResponse(result.id, result.schema.copy()))
  }
}
