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

package org.apache.flink.table.ttjoin

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.runtime.state.AbstractKeyedStateBackend
import org.apache.flink.streaming.api.operators.{AbstractStreamOperator, OneInputStreamOperator}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback
import org.apache.flink.table.api.Trigger
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.util.Collector

class MultiJoinProcessorOperator(
    val isEventTime: Boolean,
    val trigger: Trigger.Value,
    val triggerPeriod: Long,
    val groupCellsMap: Array[Array[Int]],
    val tableCount: Int,
    val tableFieldTypes: Array[Array[TypeInformation[_]]],
    val totalKeyOrder: Array[Int],
    val tableOrderKeyKeySetMap: Array[Array[Int]],
    val outputMap: Array[Array[Int]])
  extends AbstractStreamOperator[CRow]
  with OneInputStreamOperator[JoinRecord, CRow] {

  @transient
  var keyedStateBackend: AbstractKeyedStateBackend[Int] = _

  @transient
  var startKeyGroup: Int = _

  @transient
  var endKeyGroup: Int = _

  @transient
  var joinCollector: Collector[CRow] = _

  @transient
  var cellJoin: CubeCellJoin = _

  @transient
  var watermark: Long = _

  @transient
  var lock: AnyRef = _

  override def open(): Unit = {
    keyedStateBackend = getKeyedStateBackend[Int]().asInstanceOf[AbstractKeyedStateBackend[Int]]
    startKeyGroup = keyedStateBackend.getKeyGroupRange.getStartKeyGroup
    endKeyGroup = keyedStateBackend.getKeyGroupRange.getEndKeyGroup

    // create serializers
    val tableFieldSerializers = new Array[Array[TypeSerializer[_]]](tableCount)
    var tableIndex = 0
    while (tableIndex < tableCount) {
      val fieldTypes = tableFieldTypes(tableIndex)
      val fieldSerializers = new Array[TypeSerializer[_]](fieldTypes.length)
      var fieldIndex = 0
      while (fieldIndex < fieldTypes.length) {
        fieldSerializers(fieldIndex) = fieldTypes(fieldIndex).createSerializer(getExecutionConfig)
        fieldIndex += 1
      }
      tableFieldSerializers(tableIndex) = fieldSerializers
      tableIndex += 1
    }

    val out = output
    val outRow = new CRow()
    val outRecord = new StreamRecord[CRow](outRow)

    val collector = new Collector[CRow] {

      override def collect(record: CRow): Unit = {
        out.collect(outRecord)
      }

      override def close(): Unit = out.close()
    }

    cellJoin = new CubeCellJoin(
      getRuntimeContext,
      collector,
      outRow,
      tableCount,
      tableFieldSerializers,
      isEventTime && trigger == Trigger.PERIODIC_TRIGGER,
      trigger != Trigger.STREAM_TRIGGER,
      totalKeyOrder,
      tableOrderKeyKeySetMap,
      outputMap
    )

    watermark = Long.MinValue

    lock = new Object

    // trigger joining in fixed interval
    if (trigger == Trigger.PERIODIC_TRIGGER) {

      val cb: ProcessingTimeCallback = new ProcessingTimeCallback() {
        override def onProcessingTime(timestamp: Long): Unit = {

          lock.synchronized {
            periodicJoin()
          }

          getProcessingTimeService.registerTimer(
            getProcessingTimeService.getCurrentProcessingTime + triggerPeriod, this)
        }
      }

      cb.onProcessingTime(0L)
    }
  }

  private def periodicJoin(): Unit = {
    var cell = startKeyGroup
    while (cell <= endKeyGroup) {
      // set key context
      keyedStateBackend.setCurrentKey(cell, cell)
      // join
      cellJoin.join()
      cell += 1
    }

    if (isEventTime) {
      output.emitWatermark(new Watermark(watermark))
    }
  }

  override def setKeyContextElement1(record: StreamRecord[_]): Unit = {
    // we set multiple key contexts during element processing
  }

  override def processElement(element: StreamRecord[JoinRecord]): Unit = {
    if (trigger == Trigger.PERIODIC_TRIGGER) {
      lock.synchronized {
        runProcessElement(element)
      }
    } else {
      runProcessElement(element)
    }
  }

  def runProcessElement(element: StreamRecord[JoinRecord]): Unit = {
    val record = element.getValue
    val timestamp = element.getTimestamp
    val tableIdx = record.tableIdx

    // drop late events
    if (isEventTime && timestamp <= watermark) {
      throw new IllegalStateException("Join does not support late events.")
    }

    val cells = groupCellsMap(record.cubeGroupId)
    val isInsert = record.typeFlag == JoinRecord.INSERT
    val serializedRow = record.serializedRow

    // pre-process record for all cells
    cellJoin.insertRecord(tableIdx, isInsert, timestamp, serializedRow)

    // process record in each cell
    var i = 0
    while (i < cells.length) {
      val cell = cells(i)
      // check if operator is responsible for this cell
      if (cell >= startKeyGroup && cell <= endKeyGroup) {
        // set key context
        keyedStateBackend.setCurrentKey(cell, cell)
        // insert/delete record
        cellJoin.storeRecord()
        // join immediately
        if (trigger == Trigger.STREAM_TRIGGER) {
          cellJoin.join()
        }
      }

      i += 1
    }
  }

  override def processWatermark(mark: Watermark): Unit = {
    watermark = Math.max(watermark, mark.getTimestamp)

    // watermark trigger
    // joins current delta and emits watermark
    if (trigger == Trigger.WATERMARK_TRIGGER) {
      var cell = startKeyGroup
      while (cell <= endKeyGroup) {
        // set key context
        keyedStateBackend.setCurrentKey(cell, cell)
        // join
        cellJoin.join()
        cell += 1
      }
      output.emitWatermark(mark)
    }
    // periodic trigger
    // moves records from time store to delta store
    // set watermark for emitting later!
    else if (trigger == Trigger.PERIODIC_TRIGGER && isEventTime) {

      lock.synchronized {

        // pre-process watermark for all cells
        cellJoin.insertWatermark(mark.getTimestamp)

        var cell = startKeyGroup
        while (cell <= endKeyGroup) {
          // set key context
          keyedStateBackend.setCurrentKey(cell, cell)
          // make progress for tables
          cellJoin.storeProgress()
          cell += 1
        }

      }
    }
    // stream trigger
    // emits only watermark
    else {
      output.emitWatermark(mark)
    }
  }

  // finally flush the buffers
  override def close(): Unit = {
    if (trigger == Trigger.PERIODIC_TRIGGER) {
      periodicJoin()
    }
  }
}
