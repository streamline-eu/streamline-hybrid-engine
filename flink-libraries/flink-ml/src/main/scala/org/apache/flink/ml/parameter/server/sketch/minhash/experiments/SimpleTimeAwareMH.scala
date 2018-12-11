package org.apache.flink.ml.parameter.server.sketch.minhash.experiments

import org.apache.flink.ml.parameter.server.sketch.utils.TimeAwareTweetReader
import org.apache.flink.ml.parameter.server.sketch.utils.Utils.Vector
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object SimpleTimeAwareMH {

  def main(args: Array[String]): Unit = {
    val srcFile = args(0)
    val searchWords = args(1)
    val delimiter = args(2)
    val modelFile = args(3)
    val numHashes = args(4).toInt
    val timeStamp = args(5).toLong
    val windowSize = args(6).toInt
    val gapSize = args(7).toLong

    val env = StreamExecutionEnvironment.getExecutionEnvironment


    val words = scala.io.Source.fromFile(searchWords).getLines.toList

    val tweets = env
      .readTextFile(srcFile)
      .flatMap(new TimeAwareTweetReader(delimiter, words, timeStamp, windowSize))

    tweets
      .flatMap(new RichFlatMapFunction[(String, Array[String], Int), (Long, Int, Array[Int], String)] {
        override def flatMap(value: (String, Array[String], Int), out: Collector[(Long, Int, Array[Int], String)]): Unit = {
          val (id, tweetText) = (value._1, value._2)

          val a = new Vector(numHashes)
          (0 until numHashes).foreach(i =>
            a(i) = scala.util.hashing.MurmurHash3.stringHash(id, i)
          )

          for(word <- tweetText) {
            out.collect((word.hashCode, value._3, a, id))
          }
        }

        override def close(): Unit = {
          Thread.sleep(gapSize * 1000)
        }
      })
      .keyBy(x => (x._1, x._2))
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(gapSize)))
      .process(new ProcessWindowFunction[(Long, Int, Array[Int], String), String, (Long, Int), TimeWindow] {
        override def process(key: (Long, Int), context: Context,
                             elements: Iterable[(Long, Int, Array[Int], String)],
                             out: Collector[String]): Unit = {


          // TODO consult with @Gabor and refactor
          var model: Option[Array[(Long, Int)]] = None

          elements
              .map(e => (e._3, e._4.hashCode.toLong))
              .foreach{case(hashValues, tweetId) =>
                var changed = false
                val update: Array[(Long, Int)] = model match {
                  case Some(param) =>
                    param
                      .view
                      .zipWithIndex
                      .map{
                        case((storedTweetId, storedHashValue), index) =>
                          val newHashValue = hashValues(index)
                          if(storedHashValue <= newHashValue)
                            (storedTweetId, storedHashValue)
                          else {
                            changed = true
                            (tweetId, newHashValue)
                          }
                      }.toArray
                  case None =>
                    changed = true
                    hashValues.map((tweetId, _))
                }

                if(changed)
                  model = Some(update)
              }



          out.collect(s"$key:${model.mkString(",")}")
        }
      })
      .writeAsText(modelFile, FileSystem.WriteMode.OVERWRITE).setParallelism(1)

    env.execute()
  }

}
