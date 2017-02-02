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

import java.lang.reflect.Modifier
import java.util.concurrent.locks.{ReadWriteLock, ReentrantReadWriteLock}

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

/**
  * Trait for components that take inputs. This also provides an internal [[SlotMap]] to store
  * input values attached to the instance.
  */
trait Inputs {

  /** Internal slot map for user-supplied values. */
  private val inputSlotMap: SlotMap = SlotMap.empty

  /** The read-write lock for [[inputSlotMap]] */
  protected val inputLock: ReadWriteLock = new ReentrantReadWriteLock()

  /** The [[inputSlotMap]] just got updated */
  @volatile private var hasNewInput: Boolean = true

  /**
    * Returns all slots sorted by their names. The default implementation uses Java reflection to
    * list all public methods that have no arguments and return [[Slot]].
    */
  private lazy val _inputs: Array[InputSlot[_]] = getSlotMembers[InputSlot[_]]

  /////////////////////////////////////////////////////////////////////////////
  // Public APIs
  /////////////////////////////////////////////////////////////////////////////

  /** Tests whether this instance contains a slot with a given name. */
  final def hasInput(slotName: String): Boolean = {
    _inputs.exists(_.name == slotName)
  }

  /** Gets a slot by its name. */
  final def getInput(slotName: String): InputSlot[Any] = {
    _inputs.find(_.name == slotName).getOrElse {
      throw new NoSuchElementException(s"Slot $slotName does not exist.")
    }
  }

  /////////////////////////////////
  // set value for an input slot
  /////////////////////////////////

  /** Sets a slot in the embedded slot map. */
  final def input[T](slot: InputSlot[T], value: StageInput[T]): this.type = {
    shouldOwn(slot)
    value match {
      case OrdinaryInput(v) =>
        if (slot.messageClass.isPrimitive || v.getClass.isPrimitive) {
          require(slot.messageClass.getSimpleName.toLowerCase() == v.getClass.getSimpleName.toLowerCase(),
            s"$toString: Input slot ${slot.name} needs type ${slot.messageClass.getName}, but got value " +
              s"${v.toString} of type ${v.getClass.getName}")
        } else {
          require(slot.messageClass.isAssignableFrom(v.getClass),
            s"$toString: Input slot ${slot.name} needs type ${slot.messageClass.getName}, but got value " +
              s"${v.toString} of type ${v.getClass.getName}")
        }
        slot.validator.check(v, s"$toString")
      case _ =>
    }

    inputLock.writeLock().lock()
    try {
      inputSlotMap.put(slot, value)
      hasNewInput = true
    } finally {
      inputLock.writeLock().unlock()
    }
    this
  }

  /**
    * Conveniently set a present value for the given input slot
    * input(s, v: T) is equivalent to input(s, PresentOrFuture(v))
    */
  final def input[T](slot: InputSlot[T], value: T): this.type = {
    input(slot, StageInput(value))
  }

  /////////////////////////////////
  // get value of an input slot
  /////////////////////////////////

  /** Get the [[StageInput]] value of the given slot
    * Throws exception if it is not specified
    */
  final def input[T](slot: InputSlot[T]): StageInput[T] = {
    inputOption(slot) match {
      case Some(t) => t
      case None =>
        throw new NoSuchElementException(s"$toString: Input slot ${slot.name} is undefined")
    }
  }

  /**
    * Optionally get the [[StageInput]] value of the given slot
    */
  final def inputOption[T](slot: InputSlot[T]): Option[StageInput[T]] = {
    shouldOwn(slot)
    inputSlotMap.get(slot)
  }

  /**
    * Run some work on all the input, return a Future[R] where R is the return type of the work.
    * Throw exception if some input is missing, or the input is of the wrong type
    *
    * NOTE: This is reading on the input maps, so callers of this method should acquire
    * the readLock() on [[inputLock]]
    * See the code example in [[isInputChanged]] for a typical use-case.
    */
  final protected def withAllInputs[R](work: SlotValueMap => R)(implicit ec: ExecutionContext): Future[R] = {
    val v = _inputs.map(inp => input(inp).getFuture).toSeq
    Future.sequence(v).map { seq =>
      val inputVals = seq.zip(_inputs).map { case (m, s) =>
        if (s.messageClass.isPrimitive || m.getClass.isPrimitive) {
          require(s.messageClass.getSimpleName.toLowerCase() == m.getClass.getSimpleName.toLowerCase(),
            s"Stage $toString: invalid input type at slot ${s.name}, " +
              s"expected a ${s.messageClass.getSimpleName}, got ${m.getClass.getSimpleName}")
        } else {
          require(s.messageClass.isAssignableFrom(m.getClass),
            s"Stage $toString: invalid input type at slot ${s.name}, " +
              s"expected a ${s.messageClass.getSimpleName}, got ${m.getClass.getSimpleName}")
        }
        s -> m
      }
      work(SlotValueMap(inputVals))
    }
  }

  /**
    * Check if some input has been changed, useful for caching the output if the input doesn't change.
    * Typical usage should be:
    * {{{
    * inputLock.readLock().lock()
    * try {
    *   if (isInputChanged) {
    *     withAllInputs { inps =>
    *       // re-compute the output
    *     }
    *     inputUnchanged()
    *   }
    * } finally {
    *   inputLock.readLock().unlock()
    * }
    * }}}
    */
  final protected def isInputChanged: Boolean = {
    hasNewInput || _inputs.map(s => input(s)).exists {
      case o: StageOutput[_] => o.isNewOutput
      case _ => false
    }
  }

  /** Reset the [[isInputChanged]] flag */
  final protected def inputUnchanged(): Unit = {
    hasNewInput = false
    _inputs.map(s => input(s)).foreach {
      case o: StageOutput[_] => o.seen()
      case _ =>
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  // Helpers
  /////////////////////////////////////////////////////////////////////////////

  /** Validates that the input slot belongs to this instance. */
  private def shouldOwn(slot: InputSlot[_]): Unit = {
    require(hasInput(slot.name), s"Slot ${slot.name} does not belong to $toString")
  }

  /** Get all members of this class
    * that are subclasses of type T (which can be either [[InputSlot]] or [[OutputSlot]]
    */
  final protected def getSlotMembers[T <: Slot[_]](implicit tag: ClassTag[T]): Array[T] = {
    this.getClass.getMethods.filter { m =>
      Modifier.isPublic(m.getModifiers) &&
        tag.runtimeClass.asInstanceOf[Class[T]].isAssignableFrom(m.getReturnType) &&
        m.getParameterTypes.isEmpty
    }.sortBy(_.getName)
      .map(m => m.invoke(this).asInstanceOf[T])
  }

  /** Create an **input** slot of the given type */
  final protected def inputSlot[T](name: String, doc: String, defaultValue: Option[T],
                                   validator: SlotValidator[T] = SlotValidators.default[T])
                                  (implicit tag: ClassTag[T]): InputSlot[T] = {
    InputSlot[T](name, doc, defaultValue, validator)
  }
}
