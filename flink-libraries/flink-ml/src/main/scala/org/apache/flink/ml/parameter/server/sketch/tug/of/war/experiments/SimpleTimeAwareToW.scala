package org.apache.flink.ml.parameter.server.sketch.tug.of.war.experiments

import org.apache.flink.ml.parameter.server.sketch.utils.TimeAwareTweetReader
import net.openhft.hashing.LongHashFunction
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object SimpleTimeAwareToW {

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
      .flatMap(new RichFlatMapFunction[(String, Array[String], Int), (Long, Int, Array[Long])] {
        override def flatMap(value: (String, Array[String], Int), out: Collector[(Long, Int, Array[Long])]): Unit = {
          val id = value._1.toLong
          val tweet = value._2

          val hashArray = (for (i <- 0 to math.ceil(numHashes / 64).toInt) yield LongHashFunction.xx(i).hashLong(id)).toArray


          for(word <- tweet) {
            out.collect((word.hashCode, value._3, hashArray))
          }
        }

        override def close(): Unit = {
          Thread.sleep(gapSize * 1000)
        }
      })
      .keyBy(x => (x._1, x._2))
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(gapSize)))
      .process(new ProcessWindowFunction[(Long, Int, Array[Long]), String, (Long, Int), TimeWindow] {
        override def process(key: (Long, Int), context: Context,
                             elements: Iterable[(Long, Int, Array[Long])],
                             out: Collector[String]): Unit = {

          //TODO check if good
          val model = new Array[Int](numHashes)

          elements
            .map(x => collection.mutable.BitSet.fromBitMask(x._3))
            .foreach(update => {
              for(i <- 0 until numHashes){
                if(update(i)){
                  model(i) += 1
                }
                else{
                  model(i) -= 1
                }
              }
            })


          out.collect(s"$key:${model.mkString(",")}")
        }
      })
      .writeAsText(modelFile, FileSystem.WriteMode.OVERWRITE).setParallelism(1)

    env.execute()
  }
}
