package org.apache.flink.ml.parameter.server.client.sender

import org.apache.flink.ml.parameter.server.WorkerSender
import org.apache.flink.ml.parameter.server.entities._

class SimpleWorkerSender[P] extends WorkerSender[WorkerToPS[P], P] {

  override def onPull(id: Int, collectAnswerMsg: WorkerToPS[P] => Unit, partitionId: Int): Unit = {
    collectAnswerMsg(WorkerToPS(partitionId, Left(Pull(id))))
  }

  override def onPush(id: Int, deltaUpdate: P, collectAnswerMsg: WorkerToPS[P] => Unit, partitionId: Int): Unit = {
    collectAnswerMsg(WorkerToPS(partitionId, Right(Push(id, deltaUpdate))))
  }
}
