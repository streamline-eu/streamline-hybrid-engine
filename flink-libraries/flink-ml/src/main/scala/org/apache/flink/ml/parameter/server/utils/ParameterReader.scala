package org.apache.flink.ml.parameter.server.utils

import org.apache.flink.api.java.utils.ParameterTool

object ParameterReader {

  def getParams(args: Array[String]): ParameterTool = {
    def parameterCheck(arguments: Array[String]): Option[String] = {
      def outputNoParamMessage(): Unit = {
        val noParamMsg = "\tUsage:\n\n\t./run <path to parameters file>"
        println(noParamMsg)
      }

      if (arguments.length == 0 || !(new java.io.File(arguments(0)).exists)) {
        outputNoParamMessage()
        None
      } else {
        Some(arguments(0))
      }
    }

    val propsPath = parameterCheck(args).get

    ParameterTool.fromPropertiesFile(propsPath)
  }
}
