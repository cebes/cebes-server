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
 */

package io.cebes.pipeline.models

import scala.collection.mutable
import scala.concurrent.Future
import scala.reflect.ClassTag

/**
  * A Slot is a "placeholder" for receiving (input) or sending (output) messages in a Pipeline.
  * A Pipeline Component having a Slot[DataframeMessage] as its input
  * means that it expects the message that that slot to be of type [DataframeMessage].
  * Subclasses of [[Slot]] are specific cases of those slots
  */
abstract class Slot[+T <: PipelineMessage](val name: String, val doc: String)(implicit tag: ClassTag[T]) {
  def messageClass[U >: T]: Class[U] = tag.runtimeClass.asInstanceOf[Class[U]]
}

case class DataframeSlot(override val name: String = "df",
                         override val doc: String = "Dataframe Slot")
  extends Slot[DataframeMessage](name, doc)

case class SampleSlot(override val name: String = "sample",
                      override val doc: String = "DataSample Slot")
  extends Slot[SampleMessage](name, doc)

case class ModelSlot(override val name: String = "model",
                     override val doc: String = "Model Slot")
  extends Slot[ModelMessage](name, doc)

case class ValueSlot(override val name: String = "value",
                     override val doc: String = "Value Slot")
  extends Slot[ValueMessage](name, doc)

/**
  * A map of slots to the actual values.
  * This is similar to [[ParamMap]], however we use Future[T] for the actual value of the slots
  */
class SlotMap(private val map: mutable.Map[Slot[PipelineMessage], Future[PipelineMessage]]) {

  def this() = this(mutable.Map.empty)

  /**
    * Puts a (slot, value) pair (overwrites if the slot exists).
    */
  def put[T <: PipelineMessage](slot: Slot[T], value: Future[T]): this.type = {
    map(slot.asInstanceOf[Slot[T]]) = value
    this
  }

  /**
    * Optionally returns the value associated with a slot.
    */
  def get[T <: PipelineMessage](slot: Slot[T]): Option[Future[T]] = {
    map.get(slot.asInstanceOf[Slot[T]]).asInstanceOf[Option[Future[T]]]
  }

  /**
    * Returns the value associated with a slot or a default value.
    */
  def getOrElse[T <: PipelineMessage](slot: Slot[T], default: Future[T]): Future[T] = {
    get(slot).getOrElse(default)
  }

  /**
    * Gets the value of the slot or its default value if it does not exist.
    * Raises a [[NoSuchElementException]] if there is no value associated with the given slot.
    */
  def apply[T <: PipelineMessage](slot: Slot[T]): Future[T] = {
    get(slot).getOrElse {
      throw new NoSuchElementException(s"Slot named ${slot.name} is not specified.")
    }
  }
}

object SlotMap {

  /**
    * Returns an empty slot map.
    */
  def empty: SlotMap = new SlotMap()

}

/** A descriptor of slot at index `idx` of stage with name `parent` */
case class SlotDescriptor(parent: String, idx: Int)

object SlotDescriptor {

  /**
    * Parse a string of format stage[:slot] into a [[SlotDescriptor]] object
    */
  def apply(descriptor: String): SlotDescriptor = {
    ParamValidators.slotDescriptorRegex.findFirstMatchIn(descriptor) match {
      case None =>
        throw new IllegalArgumentException(s"Invalid slot descriptor: $descriptor")
      case Some(result) =>
        Option(result.group(2)) match {
          case None => new SlotDescriptor(result.group(0), 0)
          case Some(idx) => new SlotDescriptor(result.group(0), idx.toInt)
        }
    }
  }
}
