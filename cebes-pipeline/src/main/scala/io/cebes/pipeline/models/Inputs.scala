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
trait Inputs extends HasInputSlots {

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
  protected lazy val _inputs: Array[InputSlot[_]] = getSlotMembers[InputSlot[_]]

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
  def input[T](slot: InputSlot[T], value: StageInput[T]): this.type = {
    shouldOwn(slot)

    inputLock.writeLock().lock()
    try {
      inputSlotMap.put(slot, value)
      hasNewInput = true
    } catch {
      case ex: IllegalArgumentException =>
        throw new IllegalArgumentException(s"$toString: ${ex.getMessage}", ex)
    } finally {
      inputLock.writeLock().unlock()
    }
    this
  }

  /**
    * Conveniently set a present value for the given input slot
    * calling `input(s, v: T)` is equivalent to `input(s, StageInput(v))`
    */
  final def input[T](slot: InputSlot[T], value: T): this.type = {
    input(slot, StageInput(value))
  }

  /////////////////////////////////
  // get value of an input slot
  /////////////////////////////////

  /** Get the [[StageInput]] value of the given slot
    * Throws [[NoSuchElementException]] if it is not specified
    * Note that this will throw exception whenever the value is not specified,
    * even when the given slot was declared with `optional=true`.
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
    // only takes input slots that present or not optional
    val v = _inputs.filter(s => !s.optional || inputOption(s).nonEmpty).map(s => input(s).getFuture).toSeq
    Future.sequence(v).map { seq =>
      val inputVals = seq.zip(_inputs).map { case (m, s) =>
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
    hasNewInput || _inputs.flatMap(inputOption(_)).exists {
      case o: StageOutput[_] =>
        // either this is a new output (newly computed and o.isNewOutput is true)
        // or it should be recomputed eventually
        o.isNewOutput || o.stage.shouldRecompute(o.stage.getOutput(o.outputName))
      case _ => false
    }
  }

  /** Reset the [[isInputChanged]] flag */
  final protected def inputUnchanged(): Unit = {
    hasNewInput = false
    _inputs.flatMap(inputOption(_)).foreach {
      case o: StageOutput[_] => o.setNewOutput(false)
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
      .map { m =>
        val slot = m.invoke(this).asInstanceOf[T]
        require(slot.name == m.getName, s"${getClass.getSimpleName}: inconsistent slot name: " +
          s"variable named ${m.getName}, slot named ${slot.name}")
        slot
      }
  }
}
