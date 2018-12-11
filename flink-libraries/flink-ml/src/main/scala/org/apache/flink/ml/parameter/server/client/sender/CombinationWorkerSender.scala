package org.apache.flink.ml.parameter.server.client.sender

import org.apache.flink.ml.parameter.server.WorkerSender
import org.apache.flink.ml.parameter.server.common.{Combinable, CombinationLogic}
import org.apache.flink.ml.parameter.server.entities._

import scala.collection.mutable.ArrayBuffer

class CombinationWorkerSender[P](condition: (List[Combinable[WorkerToPS[P]]]) => Boolean,
                                 combinables: List[Combinable[WorkerToPS[P]]])
  extends CombinationLogic[WorkerToPS[P]](condition, combinables)
    with WorkerSender[Array[WorkerToPS[P]], P]
    with Serializable {

  override def onPull(id: Int, collectAnswerMsg: Array[WorkerToPS[P]] => Unit, partitionId: Int): Unit = {
    logic(
      (array: ArrayBuffer[WorkerToPS[P]]) => {
        array += WorkerToPS(partitionId, Left(Pull(id)))
      },
      collectAnswerMsg
    )
  }

  override def onPush(id: Int,
                      deltaUpdate: P,
                      collectAnswerMsg: Array[WorkerToPS[P]] => Unit,
                      partitionId: Int): Unit = {
    logic(
      (array: ArrayBuffer[WorkerToPS[P]]) => {
        array += WorkerToPS(partitionId, Right(Push(id, deltaUpdate)))
      },
      collectAnswerMsg
    )
  }

}
