package org.apache.flink.ml.parameter.server.client.sender

import org.apache.flink.ml.parameter.server.common.CountLogic
import org.apache.flink.ml.parameter.server.entities.WorkerToPS

case class CountClientSender[P](max: Int)
  extends CountLogic[WorkerToPS[P]](max)
    with Serializable