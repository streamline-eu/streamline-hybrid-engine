package org.apache.flink.ml.parameter.server.server.sender

import org.apache.flink.ml.parameter.server.common.TimerLogic
import org.apache.flink.ml.parameter.server.entities.PSToWorker

import scala.concurrent.duration.FiniteDuration

case class TimerPSSender[P](intervalLength: FiniteDuration)
  extends TimerLogic[PSToWorker[P]](intervalLength)
    with Serializable