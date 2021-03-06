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

import scala.collection.mutable
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

  /** The list of input slots that have just got updated */
  @volatile private var newInputs = mutable.LinkedHashSet[InputSlot[_]]()

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
      newInputs += slot
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
    * calling `input(s, v: T)` is equivalent to `input(s, OrdinaryInput(v))`
    */
  final def input[T](slot: InputSlot[T], value: T): this.type = {
    input(slot, OrdinaryInput(value))
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
    * Get the list of input slots that are "new" (having new values)
    *
    * @return list of input slot names
    */
  def getNewInputFlag: Seq[String] = {
    inputLock.writeLock().lock()
    try {
      newInputs.map(_.name).toSeq
    } finally {
      inputLock.writeLock().unlock()
    }
  }

  /**
    * set the given slots to be "new", i.e. having new values assigned to them.
    */
  def setNewInputFlag(slotNames: Seq[String]): this.type = {
    inputLock.writeLock().lock()
    try {
      newInputs.clear()
      slotNames.map(getInput).foreach(newInputs.add)
    } finally {
      inputLock.writeLock().unlock()
    }
    this
  }

  /////////////////////////////////
  // serialization
  /////////////////////////////////

  /**
    * Returns all the input values of this instance, along with the name of the input slot
    *
    * @param onlyStatefulInput whether to only consider stateful input slot
    * @return a map from slot name -> value
    */
  def getInputs(onlyStatefulInput: Boolean = false): Map[String, Any] = {
    try {
      inputLock.readLock().lock()

      val slots = if (onlyStatefulInput) {
        _inputs.filter(_.stateful)
      } else {
        _inputs
      }

      slots.flatMap { slot =>
        inputOption(slot).map {
          case stageOut: StageOutput[_] =>
            slot.name -> SlotDescriptor(parent = stageOut.stage.getName, name = stageOut.outputName)
          case ordinary: OrdinaryInput[_] =>
            slot.name -> ordinary.get
        }
      }.toMap
    } finally {
      inputLock.readLock().unlock()
    }

  }

  /**
    * Set the input slots for this instance
    * This function will ignore any input that are [[SlotDescriptor]]. Those should be considered in the [[Pipeline]]
    *
    * @param data a map from slot name to the actual value
    */
  def setInputs(data: Map[String, Any]): this.type = {
    data.foreach { case (inpName, value) =>
      require(hasInput(inpName), s"Input name $inpName not found in stage $toString")
      value match {
        case _: SlotDescriptor => // will be connected later
        case v => input(getInput(inpName), v)
      }
    }
    this
  }

  /////////////////////////////////
  // protected helpers, used by classes extending this trait
  /////////////////////////////////

  /**
    * Run some work on all the input, return a Future[R] where R is the return type of the work.
    * Throw exception if some input is missing, or the input is of the wrong type
    *
    * NOTE: This is reading on the input maps, so callers of this method should acquire
    * the readLock() on [[inputLock]]
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
    *
    * @param statefulOnly whether to only check stateful input slots
    */
  final protected def isInputChanged(statefulOnly: Boolean): Boolean = {
    newInputs.exists(!statefulOnly || _.stateful == statefulOnly) ||
      _inputs.filter(!statefulOnly || _.stateful == statefulOnly).flatMap(inputOption(_)).exists {
        case o: StageOutput[_] =>
          // either this is a new output (newly computed and o.isNewOutput is true)
          // or it should be recomputed eventually
          o.isNewOutput || o.stage.shouldRecompute(o.stage.getOutput(o.outputName))
        case _ => false
      }
  }

  /** Reset the [[isInputChanged]] flag */
  final protected def inputUnchanged(): Unit = {
    newInputs.clear()
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
