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
import org.apache.flink.runtime.state.{AbstractKeyedStateBackend, StateSnapshotContext}
import org.apache.flink.streaming.api.operators.{AbstractStreamOperator, OneInputStreamOperator}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.util.Collector

class MultiJoinProcessorOperator(
    val isEventTime: Boolean,
    val isConsistentBatching: Boolean,
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
  var watermarks: Array[Long] = _

  @transient
  var combinedWatermark: Long = _

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
      isEventTime,
      isConsistentBatching,
      totalKeyOrder,
      tableOrderKeyKeySetMap,
      outputMap
    )

    watermarks = new Array[Long](tableCount)
    var i = 0
    while (i < tableCount) {
      watermarks(i) = Long.MinValue
      i += 1
    }

    combinedWatermark = Long.MinValue
  }

  override def setKeyContextElement1(record: StreamRecord[_]): Unit = {
    // we set multiple key contexts during element processing
  }

  override def processElement(element: StreamRecord[JoinRecord]): Unit = {

    val record = element.getValue
    val timestamp = element.getTimestamp
    val tableIdx = record.tableIdx

    // process watermark for each cell this operator is responsible for
    if (record.typeFlag == JoinRecord.WATERMARK) {

      // determine combined watermark
      watermarks(tableIdx) = timestamp
      // find minimum of all watermarks
      var newMin = Long.MaxValue
      var i = 0
      while (i < tableCount) {
        newMin = Math.min(watermarks(i), newMin)
        i += 1
      }
      // new watermark
      if (newMin > combinedWatermark) {
        combinedWatermark = newMin

        // pre-process watermark for all cells
        cellJoin.insertWatermark(combinedWatermark)

        var cell = startKeyGroup
        while (cell <= endKeyGroup) {
          // set key context
          keyedStateBackend.setCurrentKey(cell, cell)
          // make progress for table
          cellJoin.storeProgress()
          // join
          if (!isConsistentBatching) {
            cellJoin.join()
          }
          cell += 1
        }

        // emit watermark
        if (!isConsistentBatching) {
          output.emitWatermark(new Watermark(combinedWatermark))
        }
      }
    }
    // process record
    else {

      // drop late events
      if (isEventTime && timestamp <= watermarks(tableIdx)) {
        return
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
          // join
          if (!isConsistentBatching) {
            cellJoin.join()
          }
        }

        i += 1
      }
    }
  }

  override def processWatermark(mark: Watermark): Unit = {
    throw new UnsupportedOperationException("This operator should not receive watermarks.")
  }

  override def snapshotState(context: StateSnapshotContext): Unit = {
    // toggle delta state for consistent batching
    if (isConsistentBatching) {
      var cell = startKeyGroup
      while (cell <= endKeyGroup) {

        // set key context
        keyedStateBackend.setCurrentKey(cell, cell)

        // switch to empty delta
        cellJoin.switchDelta(true)

        cell += 1
      }
    }

    // continue with snapshot
    super.snapshotState(context)
  }

  override def notifyOfCompletedCheckpoint(checkpointId: Long): Unit = {
    // skip if batching is not enabled
    if (!isConsistentBatching) {
      return
    }

    // join
    var cell = startKeyGroup
    while (cell <= endKeyGroup) {

      // set key context
      keyedStateBackend.setCurrentKey(cell, cell)

      // switch to full delta
      cellJoin.switchDelta(false)

      // join
      cellJoin.join()

      cell += 1
    }

    // emit watermark
    output.emitWatermark(new Watermark(combinedWatermark))

    // continue with notification
    super.notifyOfCompletedCheckpoint(checkpointId)
  }
}
