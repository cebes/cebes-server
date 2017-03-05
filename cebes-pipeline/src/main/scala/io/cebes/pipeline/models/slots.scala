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
import scala.reflect.ClassTag

/**
  * A Slot is a "placeholder" for receiving (input) or sending (output) messages in a Pipeline.
  * A Pipeline Component having a Slot[Dataframe] as its input
  * means that it expects the message in that slot to be of type [Dataframe].
  *
  * @param name         Name of the slot, normally has to be the same with the variable name
  * @param doc          (brief) documentation of the slot
  * @param defaultValue default value, as an [[Option]]
  * @param validator    [[SlotValidator]] instance to validate the value of the slot.
  *                     By default everything is validated.
  * @param optional     if false, complains in run-time when there is neither a value specified for this slot,
  *                     nor a default value was set.
  *                     When optional=true, the [[SlotValueMap]] passed into the `run()` function
  *                     of the stages will not contain this slot if it is not specified. Therefore,
  *                     the function has to handle the case when an `optional` slot is not presented in the
  *                     `run()` function.
  *                     By default, optional=false, meaning it always complains.
  *                     This is so to encourage developers to specify a sensible default value for the slots,
  *                     and making sure that once entered the `run()` function in the Stage, everything was already
  *                     specified. Developers will only need to check the existence of some special slots which
  *                     they explicitly marked as **optional**.
  */
abstract class Slot[+T](val name: String, val doc: String,
                        val defaultValue: Option[T],
                        val validator: SlotValidator[T] = SlotValidators.default[T],
                        val optional: Boolean = false)(implicit tag: ClassTag[T]) {

  SlotValidators.checkValidSlotName(name)
  if (defaultValue.isDefined) {
    validator.check(defaultValue.get)
  }

  /** the class of the message in this slot, i.e. Class[T],
    * but implemented with some tricks to overcome scala compiler's constraints.
    */
  def messageClass[U >: T]: Class[U] = tag.runtimeClass.asInstanceOf[Class[U]]

  /** Make sure the type of the given value is suitable to be assigned to this slot,
    * then call the validator to check if the given value is valid.
    */
  def checkInput[U >: T](value: U): Unit = {
    // it's quite tricky to do the type check here, especially for primitive types
    // because `tag.runtimeClass` will be the low-level types, while the `value` is normally
    // in the scala type system. For instance when `value` is `Int` (or `java.lang.Interger`), i.e. non-primitive,
    // the `tag.runtimeClass` will likely be the primitive type `int`
    // Therefore the most reliable way to check the type of `value` is to based
    // on the `defaultValue`, when it is specified of course.
    defaultValue match {
      case Some(dv) =>
        require(dv.getClass.isAssignableFrom(value.getClass),
          s"Invalid type at slot $name, " +
            s"expected a ${dv.getClass.getSimpleName}, got ${value.getClass.getSimpleName}")
      case None =>
        if (messageClass.isPrimitive || value.getClass.isPrimitive) {
          require(messageClass.getSimpleName.toLowerCase() == value.getClass.getSimpleName.toLowerCase(),
            s"Invalid type at slot $name, " +
              s"expected a ${messageClass.getSimpleName}, got ${value.toString} of type ${value.getClass.getSimpleName}")
        } else {
          require(messageClass.isAssignableFrom(value.getClass),
            s"Invalid type at slot $name, " +
              s"expected a ${messageClass.getSimpleName}, got ${value.getClass.getSimpleName}")
        }
    }
    validator.check(value, s"slot $name")
  }
}

case class InputSlot[+T](override val name: String, override val doc: String,
                         override val defaultValue: Option[T],
                         override val validator: SlotValidator[T] = SlotValidators.default,
                         override val optional: Boolean = false)
                        (implicit tag: ClassTag[T])
  extends Slot[T](name, doc, defaultValue, validator, optional)

case class OutputSlot[+T](override val name: String, override val doc: String,
                          override val defaultValue: Option[T],
                          override val validator: SlotValidator[T] = SlotValidators.default,
                          override val optional: Boolean = false)
                         (implicit tag: ClassTag[T])
  extends Slot[T](name, doc, defaultValue, validator, optional)

/**
  * A map of slots to the actual values, maps [[Slot]][T] into [[StageInput]][T]
  */
class SlotMap {

  private val map: mutable.Map[Slot[Any], StageInput[Any]] = mutable.Map.empty

  /**
    * Puts a (slot, value) pair (overwrites if the slot exists).
    * If the given value is an [[OrdinaryInput]], also check if
    * the given value is valid for the given slot
    * (by calling the [[Slot.checkInput()]] function on the slot)
    */
  def put[T](slot: Slot[T], value: StageInput[T]): this.type = {
    value match {
      case OrdinaryInput(v) =>
        slot.checkInput(v)
      case _ =>
    }
    map(slot.asInstanceOf[Slot[T]]) = value
    this
  }

  /**
    * Optionally returns the value associated with a slot.
    */
  def get[T](slot: Slot[T]): Option[StageInput[T]] = {
    map.get(slot.asInstanceOf[Slot[Any]]).asInstanceOf[Option[StageInput[T]]]
      .orElse(slot.defaultValue.map(v => StageInput(v)))
  }

  /**
    * Returns the value associated with a slot, or a default value.
    * This will ignore the default value given in the declaration of the slot.
    */
  def getOrElse[T](slot: Slot[T], default: StageInput[T]): StageInput[T] = {
    map.get(slot.asInstanceOf[Slot[Any]]).asInstanceOf[Option[StageInput[T]]].getOrElse(default)
  }

  /**
    * Gets the value of the slot or its default value if it does not exist.
    * Raises a [[NoSuchElementException]] if there is no value associated with the given slot.
    */
  def apply[T](slot: Slot[T]): StageInput[T] = {
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

/**
  * Map a [[Slot]][T] into the actual value of type T
  * This class is not thread-safe
  */
class SlotValueMap {

  private val map: mutable.Map[Slot[Any], Any] = mutable.Map.empty

  /**
    * Puts a (slot, value) pair (overwrites if the slot exists).
    * The given value is valid for the given slot
    * (by calling the [[Slot.checkInput()]] function on the slot)
    */
  def put[T](slot: Slot[T], value: T): this.type = {
    Option(value) match {
      case Some(v) =>
        slot.checkInput(v)
      case _ =>
    }
    map(slot.asInstanceOf[Slot[T]]) = value
    this
  }

  /**
    * Optionally returns the value associated with a slot.
    */
  def get[T](slot: Slot[T]): Option[T] = {
    map.get(slot.asInstanceOf[Slot[Any]]).asInstanceOf[Option[T]].orElse(slot.defaultValue)
  }

  /**
    * Returns the value associated with a slot, or a default value.
    * This will ignore the default value given in the declaration of the slot.
    */
  def getOrElse[T](slot: Slot[T], default: T): T = {
    map.get(slot.asInstanceOf[Slot[Any]]).asInstanceOf[Option[T]].getOrElse(default)
  }

  /**
    * Removes a key from this map, returning the value associated previously
    * with that key as an option.
    */
  def remove[T](slot: Slot[T]): Option[T] = map.remove(slot.asInstanceOf[Slot[Any]]).map(_.asInstanceOf[T])

  /** Tests whether this map contains a binding for a key. */
  def contains[T](slot: Slot[T]): Boolean = map.contains(slot.asInstanceOf[Slot[Any]])

  /**
    * Gets the value of the slot or its default value if it does not exist.
    * Raises a [[NoSuchElementException]] if there is no value associated with the given slot.
    */
  def apply[T](slot: Slot[T]): T = {
    get(slot).getOrElse {
      throw new NoSuchElementException(s"Slot named ${slot.name} is not specified.")
    }
  }

  /** Current size of the map */
  def size: Long = map.size

  /** Tests whether the map is empty. */
  def isEmpty: Boolean = map.isEmpty

  /** Applies a function `f` to all elements of this map */
  def foreach[U](f: ((Slot[_], _)) => U): Unit = map.foreach(f)
}

object SlotValueMap {

  /**
    * Returns an empty slot map.
    */
  def empty: SlotValueMap = new SlotValueMap()

  /** Construct a [[io.cebes.pipeline.models.SlotValueMap]] given the sequence of Slot and values */
  def apply(vals: Seq[(Slot[Any], Any)]): SlotValueMap = {
    val map = empty
    vals.foreach { case (s, m) =>
      map.put(s, m)
    }
    map
  }

  /** Construct a [[io.cebes.pipeline.models.SlotValueMap]] from a single pair of slot -> value */
  def apply[T](slot: Slot[T], value: T): SlotValueMap = apply(Seq((slot, value)))
}

/** A descriptor of slot with the given `name` of stage with name `parent` */
case class SlotDescriptor(parent: String, name: String)

object SlotDescriptor {

  /**
    * Parse a string of format stage[:slot] into a [[SlotDescriptor]] object
    */
  def apply(descriptor: String): SlotDescriptor = {
    SlotValidators.slotDescriptorRegex.findFirstMatchIn(descriptor) match {
      case None =>
        throw new IllegalArgumentException(s"Invalid slot descriptor: $descriptor")
      case Some(result) =>
        Option(result.group(3)) match {
          case None => new SlotDescriptor(result.group(1), "default")
          case Some(n) => new SlotDescriptor(result.group(1), n)
        }
    }
  }
}

/** Helper for creating input slots */
trait HasInputSlots {

  /** Create an **input** slot of the given type */
  final protected def inputSlot[T](name: String, doc: String, defaultValue: Option[T],
                                   validator: SlotValidator[T] = SlotValidators.default[T],
                                   optional: Boolean = false)
                                  (implicit tag: ClassTag[T]): InputSlot[T] = {
    InputSlot[T](name, doc, defaultValue, validator, optional)
  }
}

/** Helper for creating output slots */
trait HasOutputSlots {

  /** Create an **output** slot of the given type */
  final protected def outputSlot[T](name: String, doc: String, defaultValue: Option[T],
                                    validator: SlotValidator[T] = SlotValidators.default[T])
                                   (implicit tag: ClassTag[T]): OutputSlot[T] = {
    OutputSlot[T](name, doc, defaultValue, validator)
  }
}
