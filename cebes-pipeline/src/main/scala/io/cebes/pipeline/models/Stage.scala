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

import java.util.concurrent.locks.{ReadWriteLock, ReentrantReadWriteLock}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
  * A Pipeline stage, with a name, inputs, outputs
  * The Stage is designed around the concept of [[Future]].
  * Stage.output(s) is the output at slot with name `s`, of type [[Future[Any]] which can be waited on,
  * and cached if the input doesn't change.
  *
  * Note that this makes the stages to be stateful, which is somehow against the philosophy of Scala,
  * but we do it for the sake of runtime "efficiency",
  * although at the price of more code with all kinds of locks.
  */
trait Stage extends Inputs with HasOutputSlots {

  /**
    * Subclasses only need to implement this if they have stateful outputs.
    *
    * The states that need to be computed are given in `stateSlot`,
    * and the returned value type will be checked against the slot.
    *
    * @param inputs    all the inputs of the stage.
    * @param stateSlot output slot to be computed and returned
    */
  protected def computeStatefulOutput(inputs: SlotValueMap, stateSlot: OutputSlot[Any]): Any = {
    throw new NotImplementedError("computeStatefulOutputs() not implemented")
  }

  /**
    * Subclasses need to implement this to compute their outputs.
    *
    * If there is any stateful output, [[computeStatefulOutput()]] will be called
    * before this function, and values of those outputs will be given in the `states` arguments
    * All the inputs are copied to the `inputs` argument.
    *
    * @param inputs all the inputs of the stage. Input x can be accessed as `inputs(x)`
    * @param states all the stateful outputs of this stage.
    */
  protected def computeStatelessOutputs(inputs: SlotValueMap, states: SlotValueMap): SlotValueMap

  /////////////////////////////////////////////////////////////////////////////
  // name of a stage is nothing more than an input slot
  /////////////////////////////////////////////////////////////////////////////

  val name: InputSlot[String] = inputSlot[String]("name", "Name of this stage",
    Some(getClass.getSimpleName.toLowerCase), SlotValidators.isValidStageName)

  def getName: String = input(name).get

  def setName(newName: String): this.type = {
    input(name, newName)
  }

  /////////////////////////////////////////////////////////////////////////////
  // outputs
  /////////////////////////////////////////////////////////////////////////////

  /** list of all output slots produced by this component */
  private lazy val _outputs: Array[OutputSlot[_]] = getSlotMembers[OutputSlot[_]]

  /** Lock object for `outputMap` and `cachedOutput` */
  private val outputLock: ReadWriteLock = new ReentrantReadWriteLock()

  /** List that holds the outputs */
  private lazy val outputMap: Map[OutputSlot[Any], StageOutput[Any]] = _outputs.map { o =>
    o -> StageOutput(this, o.name)
  }.toMap

  private val cachedOutput: mutable.Map[OutputSlot[Any], Future[Any]] = mutable.Map.empty

  /** Gets output slot by its name. */
  final def getOutput(slotName: String): OutputSlot[Any] = {
    _outputs.find(_.name == slotName).getOrElse {
      throw new NoSuchElementException(s"Slot $slotName does not exist.")
    }
  }

  /**
    * Return the output at the given index.
    */
  def output[T](outputSlot: OutputSlot[T]): StageOutput[T] = {
    outputMap(outputSlot).asInstanceOf[StageOutput[T]]
  }

  /**
    * Clear the value of the given output slot, if it is already computed
    *
    * @param outputName name of the output slot to be cleared.
    */
  def clearOutput[T](outputName: String): this.type = {
    val outputSlot = getOutput(outputName)
    outputLock.writeLock().lock()
    try {
      cachedOutput.remove(outputSlot)
    } finally {
      outputLock.writeLock().unlock()
    }
    this
  }

  /** To be called only by [[StageOutput]] */
  def computeOutput[T](outputName: String)(implicit ec: ExecutionContext): Future[T] = {

    // throw exception early if outputName is invalid
    val outputSlot = getOutput(outputName)

    inputLock.readLock().lock()
    try {
      outputLock.readLock().lock()
      if (shouldRecompute(outputSlot)) {
        outputLock.readLock().unlock()
        outputLock.writeLock().lock()
        try {
          if (shouldRecompute(outputSlot)) {
            doComputeOutput(outputSlot)
          }

          // Downgrade by acquiring read lock before releasing write lock
          outputLock.readLock().lock()
        } finally {
          outputLock.writeLock().unlock()
        }
      }
    } finally {
      inputLock.readLock().unlock()
    }

    try {
      cachedOutput(outputSlot).asInstanceOf[Future[T]]
    } finally {
      outputLock.readLock().unlock()
    }
  }

  ////////////////////////////////////////////////////////////////////////////////////
  // Helpers
  ////////////////////////////////////////////////////////////////////////////////////

  /** Whether to re-compute the output map [[cachedOutput]]
    * We will recompute the output if one of the following is true:
    *  - No output was computed ([[cachedOutput]] is empty)
    *  - input is changed (the [[isInputChanged]] flag)
    * */
  private def shouldRecompute[T](outputSlot: OutputSlot[T]): Boolean = {
    if (outputSlot.stateful) {
      // if this is stateful output, only re-compute
      // if it is not already computed (or it was cleared)
      !cachedOutput.contains(outputSlot)
    } else {
      // if this is stateless, then re-compute if
      // it is not computed, or input has changed
      (!cachedOutput.contains(outputSlot)) || isInputChanged
    }
  }

  private def doComputeState[T](inputs: SlotValueMap, stateSlot: OutputSlot[T]): T = {
    val state = computeStatefulOutput(inputs, stateSlot)
    stateSlot.checkValue(state)
    state.asInstanceOf[T]
  }

  /**
    * Compute the output, check the types and number of output slots
    * [[cachedOutput]] is computed and updated in this function
    */
  private def doComputeOutput[T](outputSlot: OutputSlot[T])
                                (implicit ec: ExecutionContext): Unit = {
    val newOutputs = if (outputSlot.stateful) {
      val futureState = withAllInputs { inps =>
        inps.remove(name)
        doComputeState(inps, outputSlot)
      }
      Seq((outputSlot, futureState))

    } else {

      val statelessSlots = _outputs.filter(!_.stateful)
      val unavailableStateSlots = _outputs.filter(s => s.stateful && !cachedOutput.contains(s))
      val availableStateSlots = _outputs.filter(s => s.stateful && cachedOutput.contains(s))

      // it's important to use `withAllInputs` as the outer "flatMap"
      // in order to make sure the inputs are computed before the states (in futureAvailableStates)
      val futureOutputs = withAllInputs { inps =>
        inps.remove(name)
        inps
      }.flatMap { inps =>

        // compute all states
        val futureAvailableStates = Future.sequence(availableStateSlots.map(s => cachedOutput(s)).toSeq)
        futureAvailableStates.map { availableStates =>

          val unavailableStates = unavailableStateSlots.map(s => (s, doComputeState(inps, s)))
          val allStates = unavailableStates ++ availableStateSlots.zip(availableStates)

          // compute stateless outputs
          val outputs = try {
            computeStatelessOutputs(inps, SlotValueMap(allStates))
          } catch {
            case ex: IllegalArgumentException =>
              throw new IllegalArgumentException(s"$toString: ${ex.getMessage}", ex)
          }

          // check outputs
          statelessSlots.foreach { s =>
            require(outputs.contains(s), s"Stage $toString: output doesn't contain result for slot ${s.name}")
          }

          // update the outputs with the newly computed states
          unavailableStates.foreach { case (s, v) =>
            outputs.put(s, v)
          }
          outputs
        }
      }

      (statelessSlots ++ unavailableStateSlots).map { s =>
        (s, futureOutputs.map(m => m(s)))
      }.toSeq
    }

    // update cachedOutput and outputMap "new" status
    newOutputs.foreach { case (s, fv) =>
      cachedOutput.put(s, fv)
      outputMap(s).setNewOutput(true)
    }
    inputUnchanged()
  }

  override def toString: String = s"${getClass.getSimpleName}(name=${Try(getName).getOrElse("<unknown>")})"

}
