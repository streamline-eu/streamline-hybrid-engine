package org.apache.flink.ml.parameter.server.factorization.machines.imr.worker

import org.apache.flink.ml.parameter.server.factorization.machines.imr.utils.TYPES.{ItemId, Prediction}
import org.apache.flink.ml.parameter.server.matrix.factorization.utils.Vector._
import org.apache.flink.ml.parameter.server.{ParameterServerClient, WorkerLogic}


/**
  * Realize the worker logic for online factorization machine with SGD
  *
  *
  */
class PSOnlineFMPredictionWorker(psParallelism: Int)
  extends WorkerLogic[Prediction, Either[Vector, (Int, Vector)], (ItemId, Vector)] {

  /**
    * Method called when new data arrives.
    *
    * @param p
    * New data.
    * @param ps
    * Interface to ParameterServer.
    */
  override def onRecv(p: Prediction, ps: ParameterServerClient[Either[Vector, (Int, Vector)], (ItemId, Vector)]): Unit =
    ps.pull(p.id)

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
    case Left(v) => (0 until psParallelism).foreach(i => ps.push(paramId, Right((i, v))))
    case Right(r) => throw new IllegalStateException("Unexpected message")
  }
}
