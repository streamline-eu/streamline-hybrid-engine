package org.apache.flink.ml.parameter.server.client.receiver

import org.apache.flink.ml.parameter.server.WorkerReceiver
import org.apache.flink.ml.parameter.server.entities.{PSToWorker, PullAnswer}

class MultipleWorkerReceiver[P] extends WorkerReceiver[Array[PSToWorker[P]], P] {

  override def onPullAnswerRecv(msg: Array[PSToWorker[P]], pullHandler: PullAnswer[P] => Unit): Unit =
    msg.foreach {
      case PSToWorker(_, pullAns) => pullHandler(pullAns)
    }

}