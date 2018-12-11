package org.apache.flink.ml.parameter.server.factorization.machines.imr.utils

import org.apache.flink.ml.parameter.server.factorization.machines.imr.utils.TYPES.ItemId
import org.apache.flink.ml.parameter.server.matrix.factorization.utils.Vector._
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.io.{BufferedSource, Source}

/**
  *
  * @param dataFilePath
  * @param header
  * @param outputMessageCount
  * @param modelDim the dimension for the model. if it is None it means the max dimension (= 300).
  */
class FixModelSource(dataFilePath: String, header: Boolean, outputMessageCount: Option[Int] = None,
                     modelDim: Option[Int] = None, scaling: Option[Double] = None)
  extends SourceFunction[Iterable[(ItemId, Vector)]] {
  override def cancel(): Unit = ???

  val delimiter = ";"
  val listDelimiter = ","

  private def createList(reader: BufferedSource): List[(ItemId, Vector)] =
    (if (header) reader.getLines().drop(1)
    else reader.getLines())
      .map(l => {
        val q = l.split(delimiter)
        val modelVector = (modelDim match {
          case None => q(1).split(listDelimiter)
          case Some(n) if n <= 300 => q(1).split(listDelimiter).take(n)
          case _ =>
            // TODO: println shoukd be changed to warning log
            println("Model dimension should be under or equal 300")
            q(1).split(listDelimiter)
        }).map(_.toDouble)

        (q(0).toInt, (scaling match {
          case None => modelVector
          case Some(n) => modelVector.map(_ * n)
        }))
      }).toList

  override def run(ctx: SourceContext[Iterable[(ItemId, Vector)]]): Unit = outputMessageCount match {
      // line by line
    case None => createList(Source.fromFile(dataFilePath)).foreach(q => ctx.collect(Option(q)))
      // one piece
    case Some(1) => ctx.collect(createList(Source.fromFile(dataFilePath)))
      // n piece
    case Some(n) =>
      val l = createList(Source.fromFile(dataFilePath))
      l.sliding(Math.ceil(l.size / n.toDouble).toInt).foreach(ctx.collect(_))
  }
}
