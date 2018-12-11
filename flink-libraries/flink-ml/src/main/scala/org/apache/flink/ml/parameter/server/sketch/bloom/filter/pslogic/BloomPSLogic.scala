package org.apache.flink.ml.parameter.server.sketch.bloom.filter.pslogic

import org.apache.flink.ml.parameter.server.{ParameterServer, ParameterServerLogic}
import org.apache.flink.ml.parameter.server.sketch.utils.Utils._
import scala.collection.mutable

/**
  * Server logic for storing and updating the bloom filters
  */
class BloomPSLogic extends ParameterServerLogic[Vector, (Int, mutable.BitSet)]{

  val model = new mutable.HashMap[Int, mutable.BitSet]()

  override def onPullRecv(id: Int, workerPartitionIndex: Int, ps: ParameterServer[Vector, (Int, mutable.BitSet)]): Unit = ???

  override def onPushRecv(id: Int, deltaUpdate: Vector, ps: ParameterServer[Vector, (Int, mutable.BitSet)]): Unit = {

    val param = model.getOrElseUpdate(id, mutable.BitSet.empty)
    for(elem <- deltaUpdate){
      param += elem
    }
  }

  override def close(ps: ParameterServer[Vector, (Int, mutable.BitSet)]): Unit = {
    model.foreach{case(id, c) => ps.output(id, c)}
  }
}
