package org.apache.flink.ml.parameter.server.server.sender

import org.apache.flink.ml.parameter.server.PSSender
import org.apache.flink.ml.parameter.server.common.{Combinable, CombinationLogic}
import org.apache.flink.ml.parameter.server.entities.{PSToWorker, PullAnswer}

import scala.collection.mutable.ArrayBuffer

class CombinationPSSender[P](condition: (List[Combinable[PSToWorker[P]]]) => Boolean,
                             combinables: List[Combinable[PSToWorker[P]]])
  extends CombinationLogic[PSToWorker[P]](condition, combinables)
    with PSSender[Array[PSToWorker[P]], P]
    with Serializable {

  override def onPullAnswer(id: Int, value: P, workerPartitionIndex: Int, collectAnswerMsg: (Array[PSToWorker[P]]) => Unit): Unit = {
    logic(
      (array: ArrayBuffer[PSToWorker[P]]) => {
        array += PSToWorker(workerPartitionIndex, PullAnswer(id, value))
      },
      collectAnswerMsg
    )
  }

}
