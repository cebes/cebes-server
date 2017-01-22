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
 * Created by phvu on 30/12/2016.
 */

package io.cebes.persistence

/**
  * An Iterator with a close() function, to properly close the
  * underlying resources, which often is JDBC connections.
  */
trait ClosableIterator[A] extends Iterator[A] with AutoCloseable

object ClosableIterator {

  /**
    * Construct a [[ClosableIterator]] from a [[Iterator]]
    * with a no-op close() method.
    */
  def fromIterator[A](iterator: Iterator[A]): ClosableIterator[A] =
    new ClosableIterator[A] {

      override def next(): A = iterator.next()

      override def hasNext: Boolean = iterator.hasNext

      override def close(): Unit = {
        // do nothing
      }
    }
}
