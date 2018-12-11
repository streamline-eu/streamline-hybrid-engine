package org.apache.flink.ml.parameter.server.client.receiver

import org.apache.flink.ml.parameter.server.WorkerReceiver
import org.apache.flink.ml.parameter.server.entities.{PSToWorker, PullAnswer}

class SimpleWorkerReceiver[P] extends WorkerReceiver[PSToWorker[P], P] {

  override def onPullAnswerRecv(msg: PSToWorker[P], pullHandler: PullAnswer[P] => Unit): Unit =
    msg match {
      case PSToWorker(_, pullAns) => pullHandler(pullAns)
    }

}
