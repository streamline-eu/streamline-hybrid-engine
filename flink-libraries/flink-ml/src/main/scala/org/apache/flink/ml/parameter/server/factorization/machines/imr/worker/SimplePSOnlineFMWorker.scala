package org.apache.flink.ml.parameter.server.factorization.machines.imr.worker

import org.apache.flink.ml.parameter.server.factorization.machines.imr.utils.TYPES._
import org.apache.flink.ml.parameter.server.matrix.factorization.utils.Vector._
import org.apache.flink.ml.parameter.server.{ParameterServerClient, WorkerLogic}


/**
  * Realize the worker logic for online factorization machine with SGD
  *
  *
  */
class SimplePSOnlineFMWorker(learningRate: Double, psParallelism: Int)
  extends WorkerLogic[WorkerInput, Either[Vector, (Int, Vector)], (ItemId, Vector)] {

  import scala.collection.mutable
  import scala.collection.mutable._


  val paramWaitingQueue = new mutable.HashMap[ItemId,
    mutable.Queue[(WorkerInput, ArrayBuffer[(ItemId, Vector)])]]()


  /**
    * Method called when new data arrives.
    *
    * @param data
    * New data.
    * @param ps
    * Interface to ParameterServer.
    */
  override def onRecv(data: WorkerInput, ps: ParameterServerClient[Either[Vector, (Int, Vector)], (ItemId, Vector)]): Unit = {
    val waitingValues = new ArrayBuffer[(ItemId, Vector)]()
    data match {
      case t: TrainingData => Seq(t.id, t.similar).foreach(k => {
        paramWaitingQueue.getOrElseUpdate(k, mutable.Queue[(WorkerInput, ArrayBuffer[(ItemId, Vector)])]())
          .enqueue((t, waitingValues))
        ps.pull(k)
      })
      case p@Prediction(id, _) =>
        paramWaitingQueue.getOrElseUpdate(id, mutable.Queue[(WorkerInput, ArrayBuffer[(ItemId, Vector)])]())
          .enqueue((p, waitingValues))
        ps.pull(id)
    }
  }

  def deltaValues(t: TrainingData, waitingValues: ArrayBuffer[(ItemId, Vector)]): Seq[(ItemId, Vector)] = {
    val item1 = waitingValues(0)
    val item2 = waitingValues(1)
    val errorConst = -1 * learningRate * 2 * (dotProduct(item1._2, item2._2) - (t match {
      case _: Similarity => 1
      case _: NegativeSample => 0
    }))
    mutable.Seq((item1._1, item2._2.map(i => errorConst * i)), (item2._1, item1._2.map(i => errorConst * i)))

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
  override def onPullRecv(paramId: ItemId, paramValue: Either[Vector, (Int, Vector)],
                          ps: ParameterServerClient[Either[Vector, (Int, Vector)], (ItemId, Vector)]): Unit =
    paramValue match {
      case Left(v) =>
        val q = paramWaitingQueue(paramId)
        val (restData, waitingValues) = q.dequeue()
        waitingValues += paramId -> v
        restData match {
          case t: TrainingData =>
            if (waitingValues.size == 2) deltaValues(t, waitingValues).foreach { case (id, delta) => ps.push(id, Left(delta)) }
          case Prediction(id, similarityList) =>
            (0 until psParallelism).foreach(i => ps.push(id, Right((i, v))))
        }
        if (q.isEmpty) paramWaitingQueue.remove(paramId)
      case Right(r) => throw new IllegalStateException("Unexpected message")
    }
}
