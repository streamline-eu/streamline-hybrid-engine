package org.apache.flink.ml.parameter.server.sketch.bloom.filter.experiments

import java.lang.Math.floorMod

import org.apache.flink.ml.parameter.server.sketch.utils.TimeAwareTweetReader
import org.apache.flink.api.common.functions.{FlatMapFunction, RichFlatMapFunction}
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object SimpleTimeAwareBF {

  def main(args: Array[String]): Unit = {
    val srcFile = args(0)
    val searchWords = args(1)
    val delimiter = args(2)
    val modelFile = args(3)
    val numHashes = args(4).toInt
    val arraySize = args(5).toInt
    val timeStamp = args(6).toLong
    val windowSize = args(7).toInt
    val gapSize = args(8).toLong

    val env = StreamExecutionEnvironment.getExecutionEnvironment


    val words = scala.io.Source.fromFile(searchWords).getLines.toList

    val tweets = env
      .readTextFile(srcFile)
      .flatMap(new TimeAwareTweetReader(delimiter, words, timeStamp, windowSize))

    tweets
      .flatMap(new RichFlatMapFunction[(String, Array[String], Int), (Long, Int, Array[Int])] {
        override def flatMap(value: (String, Array[String], Int), out: Collector[(Long, Int, Array[Int])]): Unit = {
          val AS = (for(i <- 0 until numHashes)
            yield floorMod(scala.util.hashing.MurmurHash3.stringHash(value._1, i), arraySize))
            .toArray

          for(word <- value._2){
           out.collect(word.hashCode, value._3, AS)
          }
        }

        override def close(): Unit = {
          Thread.sleep(gapSize * 1000)
        }
      })
      .keyBy(x => (x._1, x._2))
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(gapSize)))
      .process(new ProcessWindowFunction[(Long, Int, Array[Int]), String, (Long, Int), TimeWindow] {
        override def process(key: (Long, Int), context: Context,
                             elements: Iterable[(Long, Int, Array[Int])],
                             out: Collector[String]): Unit = {
          out.collect(s"$key:${elements.flatMap(_._3).mkString(",")}")
        }
      })
      .writeAsText(modelFile, FileSystem.WriteMode.OVERWRITE).setParallelism(1)

    env.execute()
  }
}
