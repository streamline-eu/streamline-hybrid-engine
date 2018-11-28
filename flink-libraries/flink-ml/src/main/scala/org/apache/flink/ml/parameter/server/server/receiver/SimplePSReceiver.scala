package org.apache.flink.ml.parameter.server.server.receiver

import org.apache.flink.ml.parameter.server.PSReceiver
import org.apache.flink.ml.parameter.server.entities.{Pull, Push, WorkerToPS}

class SimplePSReceiver[P] extends PSReceiver[WorkerToPS[P], P] {

  override def onWorkerMsg(wToPS: WorkerToPS[P],
                           onPullRecv: (Int, Int) => Unit,
                           onPushRecv: (Int, P) => Unit): Unit = {
    wToPS.msg match {
      case Left(Pull(paramId)) =>
        // Passes key and partitionID
        onPullRecv(paramId, wToPS.workerPartitionIndex)
      case Right(Push(paramId, delta)) =>
        onPushRecv(paramId, delta)
      case _ =>
        throw new Exception("Parameter server received unknown message.")
    }
  }

}