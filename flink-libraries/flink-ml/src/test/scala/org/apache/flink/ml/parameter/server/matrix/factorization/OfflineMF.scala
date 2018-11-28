package org.apache.flink.ml.parameter.server.matrix.factorization

import java.io.{FileWriter, PrintWriter}

import org.apache.flink.ml.parameter.server.matrix.factorization.utils.Rating
import org.apache.flink.ml.parameter.server.matrix.factorization.utils.Vector.Vector
import org.apache.flink.ml.parameter.server.matrix.factorization.utils.Utils.{ItemId, UserId}
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable

object OfflineMF {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val src = env.readTextFile("thesis/input/first_9")
      .map(line => {
        Rating.fromArray(line.split(",").map(_.toLong))
      })

    PSOfflineMatrixFactorization
      .psOfflineMF(
        src,
        learningRate = 0.15, negativeSampleRate = 19,
        iterations = 4,
        workerParallelism = 4, psParallelism = 4, iterationWaitTime = 10000, pullLimit = 2200)
      .addSink(new RichSinkFunction[Either[(UserId, Vector), (ItemId, Vector)]] {
        private val users = new mutable.HashMap[UserId, Vector]
        private val items = new mutable.HashMap[ItemId, Vector]

        override def invoke(value: Either[(UserId, Vector), (ItemId, Vector)]): Unit = {
          value match {
            case Left(u) => users.update(u._1, u._2)
            case Right(i) => items.update(i._1, i._2)
          }
        }

        override def close(): Unit = {
          val output = new PrintWriter(new FileWriter("thesis/output/model/first_9/merged"))

          users
            .foreach(u => {
              output.write("u:" + u._1 + ":" + u._2.head)
              u._2
                .tail
                .foreach(p => output.write("," + p))
              output.write("\n")
            })

          items
            .foreach(i => {
              output.write("i:" + i._1 + ":" + i._2.head)
              i._2
                .tail
                .foreach(p => output.write("," + p))
              output.write("\n")
            })

          output.close()
        }
      } ).setParallelism(1)

    env.execute()
  }
}
