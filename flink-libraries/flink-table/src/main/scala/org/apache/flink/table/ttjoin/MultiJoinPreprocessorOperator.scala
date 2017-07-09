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

import java.util

import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos
import org.apache.flink.streaming.api.operators._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row
import org.apache.flink.util.MathUtils

class MultiJoinPreprocessorOperator(
    val tableIdx: Byte,
    val payloadMapping: Array[Int],
    val dimensionCount: Int,
    val dimensionSizes: Array[Int],
    val keyDimensions: Array[Int],
    val posGroupMap: util.Map[CubePosition, CubeGroup],
    val rowtimeIndex: Int)
  extends AbstractStreamOperator[JoinRecord]
  with OneInputStreamOperator[CRow, JoinRecord] {

  setChainingStrategy(ChainingStrategy.ALWAYS)

  @transient
  var payloadRow: Row = _

  @transient
  var joinRecord: JoinRecord = _

  @transient
  var streamRecord: StreamRecord[JoinRecord] = _

  @transient
  var pos: CubePosition = _

  @transient
  var random: util.Random = _

  override def open(): Unit = {

    payloadRow = new Row(payloadMapping.length)

    joinRecord = new JoinRecord(
      tableIdx,
      -1,
      -1,
      new ByteStore(ByteStore.DEFAULT_CAPACITY))

    joinRecord.row = payloadRow

    streamRecord = new StreamRecord[JoinRecord](joinRecord)

    pos = new CubePosition(dimensionCount)

    // set replicating target dimensions
    var i = 0
    while (i < dimensionCount) {
      pos.coords(i) = Int.MaxValue
      i += 1
    }

    random = new util.Random()
  }

  override def processElement(element: StreamRecord[CRow]): Unit = {
    val value = element.getValue

    // set metadata
    joinRecord.typeFlag = if (value.change) JoinRecord.INSERT else JoinRecord.DELETE
    // set the timestamp for rowtime, proctime does not set a timestamp
    if (rowtimeIndex >= 0) {
      streamRecord.setTimestamp(value.row.getField(rowtimeIndex).asInstanceOf[Long])
    }

    // convert row according to mapping
    var i = 0
    while (i < payloadMapping.length) {
      payloadRow.setField(i, value.row.getField(payloadMapping(i)))
      i += 1
    }

    // determine target cube group coordinates
    val coords = pos.coords

    // determine hash/random target dimensions
    i = 0
    while (i < keyDimensions.length) {
      val dimension = keyDimensions(i)
      // random partitioning
      if (dimension < 0) {
        // (random(), random(), *)
        val realDimension = (-1 * dimension) - 1
        coords(realDimension) = random.nextInt(dimensionSizes(realDimension))
      }
      // hash partitioning
      else if (dimension > 0) {
        // (murmur(hash(a)), murmur(hash(b)), *)
        val realDimension = dimension - 1
        coords(realDimension) =
          MathUtils.murmurHash(payloadRow.getField(i).hashCode()) % dimensionSizes(realDimension)
      }
      // replication otherwise
      i += 1
    }

    // determine target cube cells/Flink key groups of cube
    val cubeGroup = posGroupMap.get(pos)

    joinRecord.cubeGroupId = cubeGroup.groupId
    joinRecord.cells = cubeGroup.cells

    // emit
    output.collect(streamRecord)
  }

  override def processWatermark(mark: Watermark): Unit = {
    // only emit watermarks if we have an event-time join
    if (rowtimeIndex >= 0) {
      // set metadata
      joinRecord.typeFlag = JoinRecord.WATERMARK
      streamRecord.setTimestamp(mark.getTimestamp)

      // emit
      output.collect(streamRecord)
    }
  }
}
