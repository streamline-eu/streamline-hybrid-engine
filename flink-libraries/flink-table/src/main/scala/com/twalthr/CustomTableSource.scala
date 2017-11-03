/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twalthr

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.TableException
import org.apache.flink.table.sources._
import org.apache.flink.types.Row

import scala.collection.mutable

class CustomCsvTableSource(
    private val path: String,
    private val fieldNames: Array[String],
    private val fieldTypes: Array[TypeInformation[_]])
  extends StreamTableSource[Row]
  with DefinedProctimeAttribute
  with DefinedRowtimeAttribute {

  if (fieldNames.length != fieldTypes.length) {
    throw TableException("Number of field names and field types must be equal.")
  }

  private val returnType = new RowTypeInfo(fieldTypes, fieldNames)

  override def getReturnType: RowTypeInfo = returnType

  private var outOfOrder: Long = -1L

  def setOutOfOrder(amount: Long, unit: TimeUnit): Unit = {
    if (amount < 0) {
      this.outOfOrder = -1L
    } else {
      this.outOfOrder = unit.toMillis(amount)
    }
  }

  private var tableName: String = _

  def setTableName(tableName: String): Unit = {
    this.tableName = tableName
  }

  private var servingSpeed: Long = _

  def setServingSpeed(servingSpeed: Long): Unit = {
    this.servingSpeed = servingSpeed
  }

  private var delay: Long = _

  def setDelay(delay: Long): Unit = {
    this.delay = delay
  }

  override def getDataStream(streamExecEnv: StreamExecutionEnvironment): DataStream[Row] = {
    val input = streamExecEnv.addSource(new DataGenerator(tableName, path, delay, servingSpeed, true), returnType)
    if (outOfOrder != -1 && rowtime) {
      input.assignTimestampsAndWatermarks(new WatermarkAssigner(outOfOrder))
    } else {
      input // ending sources emit a final Long.MAX_VALUE watermark
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: CustomCsvTableSource => returnType == that.returnType &&
        path == that.path
    case _ => false
  }

  override def hashCode(): Int = {
    returnType.hashCode()
  }

  private var timePrefix = ""

  def setTimePrefix(prefix: String): Unit = {
    timePrefix = prefix
  }

  override def getProctimeAttribute: String = timePrefix + "proctime"

  var rowtime = false

  def setRowtime(rt: Boolean): Unit = {
    rowtime = rt
  }

  override def getRowtimeAttribute: String = if (rowtime) {
    timePrefix + "rowtime"
  } else {
    null
  }
}

object CustomCsvTableSource {

  class Builder {

    private val schema: mutable.LinkedHashMap[String, TypeInformation[_]] =
      mutable.LinkedHashMap[String, TypeInformation[_]]()
    private var path: String = _

    def path(path: String): Builder = {
      this.path = path
      this
    }

    def field(fieldName: String, fieldType: TypeInformation[_]): Builder = {
      if (schema.contains(fieldName)) {
        throw new IllegalArgumentException(s"Duplicate field name $fieldName.")
      }
      schema += (fieldName -> fieldType)
      this
    }

   def build(): CustomCsvTableSource = {
      if (path == null) {
        throw new IllegalArgumentException("Path must be defined.")
      }
      if (schema.isEmpty) {
        throw new IllegalArgumentException("Fields can not be empty.")
      }
      new CustomCsvTableSource(
        path,
        schema.keys.toArray,
        schema.values.toArray)
    }
  }

  def builder(): Builder = new Builder
}

class WatermarkAssigner(val outOfOrder: Long) extends AssignerWithPunctuatedWatermarks[Row] {

  var lastWatermark = Long.MinValue

  override def extractTimestamp(t: Row, prevTs: Long): Long = t.getField(0).asInstanceOf[Long]

  override def checkAndGetNextWatermark(t: Row, extractedTs: Long): Watermark = {
    val nextWatermark = extractedTs - outOfOrder
    if (lastWatermark < nextWatermark) {
      lastWatermark = nextWatermark
      new Watermark(lastWatermark)
    } else {
      null
    }
  }
}
