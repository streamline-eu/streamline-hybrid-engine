package org.apache.flink.ml.parameter.server.server.sender

import org.apache.flink.ml.parameter.server.common.CountLogic
import org.apache.flink.ml.parameter.server.entities.PSToWorker

case class CountPSSender[P](max: Int)
  extends CountLogic[PSToWorker[P]](max)
    with Serializable