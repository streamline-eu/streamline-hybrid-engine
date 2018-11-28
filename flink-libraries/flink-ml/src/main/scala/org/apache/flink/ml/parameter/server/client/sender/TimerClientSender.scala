package org.apache.flink.ml.parameter.server.client.sender

import org.apache.flink.ml.parameter.server.common.TimerLogic
import org.apache.flink.ml.parameter.server.entities.WorkerToPS

import scala.concurrent.duration._

case class TimerClientSender[P](intervalLength: FiniteDuration)
  extends TimerLogic[WorkerToPS[P]](intervalLength)
    with Serializable