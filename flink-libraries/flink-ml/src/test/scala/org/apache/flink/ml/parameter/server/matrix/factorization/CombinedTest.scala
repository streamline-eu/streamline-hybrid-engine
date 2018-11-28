package org.apache.flink.ml.parameter.server.matrix.factorization

import org.apache.flink.ml.parameter.server.matrix.factorization.pruning.COORD
import org.apache.flink.ml.parameter.server.matrix.factorization.utils.Rating
import org.apache.flink.ml.parameter.server.matrix.factorization.sinks.nDCGSink
import org.apache.flink.ml.parameter.server.matrix.factorization.utils.Utils.{ItemId, UserId}
import org.apache.flink.ml.parameter.server.matrix.factorization.utils.Vector.LengthAndVector
import org.apache.flink.streaming.api.scala._

object CombinedTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val user: DataStream[(UserId, LengthAndVector)] = env
      .readTextFile("thesis/output/model/first_9/users")
      .map(l => {
        val id :: params :: Nil = l.split(":").toList
        val v = params.split(",").map(_.toDouble)
        (id.toInt, (org.apache.flink.ml.parameter.server.matrix.factorization.utils.Vector.vectorLengthSqr(v), v))
      })

    val item: DataStream[(ItemId, LengthAndVector)] = env
      .readTextFile("thesis/output/model/first_9/items")
      .map(l => {
        val id :: params :: Nil = l.split(":").toList
        val v = params.split(",").map(_.toDouble)
        (id.toInt, (org.apache.flink.ml.parameter.server.matrix.factorization.utils.Vector.vectorLengthSqr(v), v))
      })


    val model: DataStream[Either[(ItemId, LengthAndVector), (UserId, LengthAndVector)]] =
      user
        .connect(item)
        .map(
          Left(_),
          Right(_))

    val src = env
      .readTextFile("thesis/input/week_10")
      .map(line => {
        val fieldsArray = line.split(",")

        Rating(fieldsArray(1).toInt, fieldsArray(2).toInt, 1.0, fieldsArray(0).toLong)
      })

    val recs = PSOnlineMFAndTopKWithModel.experiment(
      model, src,
      0.35, 4, 10,
      -0.01, 0.01,
      10,
      100, 75, 100, COORD(),
      40, 4, 4, 20000)

    nDCGSink.nDCGToFile(recs, "thesis/output/nDCG_online_10", 86400)

    env.execute()
  }

}
