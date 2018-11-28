package org.apache.flink.ml.parameter.server.matrix.factorization

import org.apache.flink.ml.parameter.server.matrix.factorization.pruning.LENGTH
import org.apache.flink.ml.parameter.server.matrix.factorization.sinks.nDCGSink
import org.apache.flink.ml.parameter.server.matrix.factorization.utils.Utils.{ItemId, UserId}
import org.apache.flink.ml.parameter.server.matrix.factorization.utils.Vector.{LengthAndVector, attachLength}
import org.apache.flink.ml.parameter.server.matrix.factorization.utils.{IDGenerator, RichRating}
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala._

object TopKGenerate {

  def main(args: Array[String]): Unit = {

    val w = "normal"
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(20)

    val model: DataStream[Either[(UserId, LengthAndVector), (ItemId, LengthAndVector)]] =
      env.readTextFile("thesis/output/model/first_9/merged")
      .map(l => {
        val typ :: id :: params :: Nil = l.split(":").toList
        val v = params.split(",").map(_.toDouble)

        typ match {
          case "i" => Right((id.toInt, attachLength(v)))
          case "u" => Left ((id.toInt, attachLength(v)))
        }
      })

    val src = env
      .readTextFile("thesis/input/week_10")
      .map(line => {
        val fieldsArray = line.split(",")

        RichRating(fieldsArray(1).toInt, fieldsArray(2).toInt, 1.0, 0, IDGenerator.next, fieldsArray(0).toLong)
      })

    val x = w match {
      case "normal" =>
        PSTopKGenerator.psTopKGenerator(
          src,
          model,
          userMemory = 0,
          psParallelism = 4,
          workerParallelism = 4,
          workerK = 50,
          iterationWaitTime = 20000,
          pullLimit = 200,
          K = 100,
          pruningAlgorithm = LENGTH())
     /* case "dummy" =>
        PSDummyTopKGenerator.experiment(
          src, model,
          psParallelism = 4, workerParallelism = 4,
          workerK = 100, K = 100,
          pullLimit = 50, iterationWaitTime = 10000)*/
    }


    x
        .map(x => (x._1, x._2, x._3.map(_._2)))
      .writeAsText("thesis/output/LEMP_exp/50", FileSystem.WriteMode.OVERWRITE).setParallelism(1)

    nDCGSink.nDCGToFile(x, "thesis/output/LEMP_exp/5_ndcg", 86400)

    env.execute()

  }
}
