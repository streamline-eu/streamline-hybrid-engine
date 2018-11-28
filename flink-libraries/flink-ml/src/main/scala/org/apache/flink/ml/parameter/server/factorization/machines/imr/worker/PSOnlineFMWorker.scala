package org.apache.flink.ml.parameter.server.factorization.machines.imr.worker

import org.apache.flink.ml.parameter.server.factorization.machines.imr.utils.TYPES.{ComplexTrainInput, ItemId, NegativeTrain, Train}
import org.apache.flink.ml.parameter.server.matrix.factorization.utils.Vector._
import org.apache.flink.ml.parameter.server.{ParameterServerClient, WorkerLogic}


/**
  * Realize the worker logic for online factorization machine with SGD
  *
  *
  */
class PSOnlineFMWorker(learningRate: Double)
  extends WorkerLogic[ComplexTrainInput, Vector, (ItemId, Vector)] {

  import scala.collection.mutable
  import scala.collection.mutable._


  val paramWaitingQueue = new mutable.HashMap[ItemId,
    mutable.Queue[(ComplexTrainInput, ArrayBuffer[(ItemId, Vector)])]]()


  /**
    * Method called when new data arrives.
    *
    * @param data
    * New data.
    * @param ps
    * Interface to ParameterServer.
    */
  override def onRecv(data: ComplexTrainInput, ps: ParameterServerClient[Vector, (ItemId, Vector)]): Unit = {
    val waitingValues = new ArrayBuffer[(ItemId, Vector)]()
    data.parameterSet.foreach(k => {
      paramWaitingQueue.getOrElseUpdate(k, mutable.Queue[(ComplexTrainInput, ArrayBuffer[(ItemId, Vector)])]())
        .enqueue((data, waitingValues))
      ps.pull(k)
    })
  }

  private def deltaValues(trainData: ComplexTrainInput, waitingValues: ArrayBuffer[(ItemId, Vector)]) = {
    val itemsMap = waitingValues.toMap
    val item1 = trainData.product.parameters.map(itemsMap(_)).reduce(vectorSum)
    val item2 = trainData.similar.parameters.map(itemsMap(_)).reduce(vectorSum)
    val errorConst = -1 * learningRate * 2 * (dotProduct(item1, item2) - (trainData match {
      case _: Train => 1
      case _: NegativeTrain => 0
    }))
    val (item2Delta, item1Delta) = (item1.map(i => errorConst * i), item2.map(i => errorConst * i))
    trainData.product.parameters.map((_, item1Delta)) ++ trainData.similar.parameters.map((_, item2Delta))
  }

  /**
    * Method called when an answer arrives to a pull message.
    * It contains the parameter.
    *
    * @param paramId
    * Identifier of the received parameter.
    * @param paramValue
    * Value of the received parameter.
    * @param ps
    * Interface to ParameterServer.
    */
  override def onPullRecv(paramId: ItemId, paramValue: Vector,
                          ps: ParameterServerClient[Vector, (ItemId, Vector)]): Unit = {
    val q = paramWaitingQueue(paramId)
    val (restData, waitingValues) = q.dequeue()
    waitingValues += paramId -> paramValue
    if (waitingValues.size == restData.parameterNumber) deltaValues(restData, waitingValues).foreach { case (id, delta) => ps.push(id, delta) }
    if (q.isEmpty) paramWaitingQueue.remove(paramId)
  }
}
