package org.apache.flink.ml.parameter.server.server.sender

import org.apache.flink.ml.parameter.server.PSSender
import org.apache.flink.ml.parameter.server.entities.{PSToWorker, PullAnswer}

class SimplePSSender[P] extends PSSender[PSToWorker[P], P]{

  override def onPullAnswer(id: Int, value: P, workerPartitionIndex: Int, collectAnswerMsg: (PSToWorker[P]) => Unit): Unit = {
    collectAnswerMsg(PSToWorker[P](workerPartitionIndex, PullAnswer(id, value)))
  }

}
