package org.apache.flink.ml.parameter.server.factorization.machines.imr.utils

import org.apache.flink.ml.parameter.server.factorization.machines.imr.utils.TYPES.{ItemId, Prediction}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.io.{BufferedSource, Source}

/**
  * Created by lukacsg on 2018.08.29..
  */
class TestSource(dataFilePath: String, header: Boolean = true) extends SourceFunction[Prediction] {
  override def cancel(): Unit = ???

  val delimiter = ";"
  val listDelimiter = ","

  private def createList(reader: BufferedSource): List[(ItemId, Array[ItemId])] =
    (if (header)  reader.getLines().drop(1)
    else reader.getLines())
      .map(l => {
        val q = l.split(delimiter)
        (q(0).toInt, q(1).split(listDelimiter).map(_.toInt))
      }).toList

  override def run(ctx: SourceContext[Prediction]): Unit = {
    val test = createList(Source.fromFile(dataFilePath))
    test.foreach{case(id, similarArray) => ctx.collect(Prediction(id, similarArray))
    }
  }
}
