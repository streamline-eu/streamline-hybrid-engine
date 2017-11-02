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

import java.lang

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.Types
import org.apache.flink.table.sinks.{RetractStreamTableSink, TableSink}
import org.apache.flink.types.Row

class CustomTableSink(query: String, path: String) extends RetractStreamTableSink[Row] {

  private var fieldNames: Array[String] = _
  private var fieldTypes: Array[TypeInformation[_]] = _

  override def getRecordType: TypeInformation[Row] = Types.ROW(fieldNames, fieldTypes)

  override def emitDataStream(dataStream: DataStream[tuple.Tuple2[lang.Boolean, Row]]): Unit = {
    dataStream.transform(
      "DataValidator",
      Types.ROW(Types.INT, Types.INT),
      new DataValidator).writeAsText(path, WriteMode.OVERWRITE)
  }

  override def getFieldNames: Array[String] = fieldNames

  override def getFieldTypes: Array[TypeInformation[_]] = fieldTypes

  override def configure(fieldNames: Array[String], fieldTypes: Array[TypeInformation[_]])
    : TableSink[tuple.Tuple2[lang.Boolean, Row]] = {
    val sink = new CustomTableSink(query, path)
    sink.fieldNames = fieldNames
    sink.fieldTypes = fieldTypes
    sink
  }
}
