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

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.checkpoint.Checkpointed
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{StreamQueryConfig, TableEnvironment}
import org.apache.flink.table.plan.stats.{ColumnStats, TableStats}
import org.apache.flink.table.runtime.utils.StreamingWithStateTestBase
import org.apache.flink.types.Row
import org.junit.{Ignore, Test}

import scala.collection.mutable

class MultiJoinITCase extends StreamingWithStateTestBase {

  @Ignore
  @Test
  def testSimpleProctimeJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setMaxParallelism(4)
    env.setParallelism(2)
    env.enableCheckpointing(5000)
    env.setStateBackend(getStateBackend)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val queryConfig = new StreamQueryConfig()

    registerComplexTestTables(env, tEnv)

    val result = tEnv
      .sql("SELECT R.rowtime, R.a, R.b, S.c, R.`value`, S.`value1`, T.`value` FROM R, S, T WHERE " +
        "JOINED_TIME(R.rowtime, S.rowtime, T.rowtime) " +
        "AND R.a = T.a AND R.b = S.b AND S.c = T.c")
      .toAppendStream[Row](queryConfig).print()

//    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

//    val expected = mutable.MutableList("Jack#22,Jack,22", "Anna#44,Anna,44")
//    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

//  @Test
//  def testSimpleJoin2(): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val tEnv = TableEnvironment.getTableEnvironment(env)
//
//    registerSimpleTestTables(env, tEnv)
//
//    val result = tEnv
//      .sql("SELECT * FROM A JOIN B ON A.a = B.a JOIN C ON B.a = C.a")
//      .toAppendStream[Row]
//
//    result.addSink(new StreamITCase.StringSink[Row])
//    env.execute()
//
//    val expected = mutable.MutableList("Jack#22,Jack,22", "Anna#44,Anna,44")
//    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
//  }

  // TODO: test one dimension
  // (e.g. SELECT * FROM R, S, T WHERE R.a = T.a AND R.b = S.b AND R.a = T.c mit R.b skewed)

  private def registerSimpleTestTables(
      env: StreamExecutionEnvironment,
      tEnv: StreamTableEnvironment)
    : Unit = {

    // R (value: String, a: Int)
    {
      val data = new mutable.MutableList[(String, Int)]
      data.+=(("R: a: 1", 1))
      data.+=(("R: a: 2", 2))
      data.+=(("R: a: 3", 3))
      data.+=(("R: a: 4", 4))
      val stream = env.fromCollection(data)
      tEnv.registerDataStream("R", stream, 'value, 'a, 'proctime.proctime)
    }

    // S (a: Int, value: String)
    {
      val data = new mutable.MutableList[(Int, String)]
      data.+=((1, "S: a: 1"))
      data.+=((2, "S: a: 2"))
      data.+=((3, "S: a: 3"))
      data.+=((4, "S: a: 4"))
      val stream = env.fromCollection(data)
      tEnv.registerDataStream("S", stream, 'a, 'value, 'proctime.proctime)
    }

    // T (a: Int, value: String)
    {
      val data = new mutable.MutableList[(Int, String)]
      data.+=((1, "T: a: 1"))
      data.+=((2, "T: a: 2"))
      // 3 is missing
      // data.+=((3, "T: a: 3"))
      data.+=((4, "T: a: 4"))
      val stream = env.fromCollection(data)
      tEnv.registerDataStream("T", stream, 'a, 'value, 'proctime.proctime)
    }
  }

  private def registerComplexTestTables(
      env: StreamExecutionEnvironment,
      tEnv: StreamTableEnvironment)
    : Unit = {

    // R (a: Int, b: Int, value: String)
    {
      val data = new mutable.MutableList[(Int, Int, String)]
      data.+=((10, 1, "R: a: 10, b: 1"))
      data.+=((10, 1, "R: a: 10, b: 1"))
      val stream = env.addSource(new DelayingSourceFunction(data))

      val stats = TableStats(1000L)
      stats.colStats.put("a", ColumnStats(1000L, null, null, null, null, null))
      stats.colStats.put("b", ColumnStats(10L, null, null, null, null, null, skewness = false))
      tEnv.registerDataStream("R", stream, stats, 'a, 'b, 'value, 'proctime.proctime,
        'rowtime.rowtime)
    }

    // S (c: Int, value1: String, value2: String, value3: String, b: Int)
    {
      val data = new mutable.MutableList[(Int, String, String, String, Int)]
      data.+=((5, "S: b: 1, c: 5", "X", "X", 1))
      data.+=((1, "S: b: 4, c: 1", "X", "X", 4))
      data.+=((4, "S: b: 4, c: 4", "X", "X", 4))
      data.+=((5, "S: b: 4, c: 5", "X", "X", 4))
      val stream = env.addSource(new DelayingSourceFunction(data))
      tEnv.registerDataStream("S", stream, 'c, 'value1, 'value2, 'value3, 'b,
        'proctime.proctime, 'rowtime.rowtime)
    }

    // T (a: Int, c: Int, value: String)
    {
      val data = new mutable.MutableList[(Int, Int, String)]
      data.+=((7, 2, "T: a: 7, c: 2"))
      data.+=((7, 3, "T: a: 7, c: 3"))
      data.+=((7, 5, "T: a: 7, c: 5"))
      data.+=((10, 5, "T: a: 10, c: 5"))
      val stream = env.addSource(new DelayingSourceFunction(data))
      tEnv.registerDataStream("T", stream, 'a, 'c, 'value, 'proctime.proctime, 'rowtime.rowtime)
    }
  }
}

class DelayingSourceFunction[T](elements: Seq[T])
  extends SourceFunction[T]
  with Checkpointed[Integer] {

  var count = 0
  @volatile
  var isRunning = true

  override def run(ctx: SourceFunction.SourceContext[T]): Unit = {
    while (isRunning && count < elements.size) {
      ctx.getCheckpointLock.synchronized {
        Thread.sleep(1000)
        println("Emitting Record: " + elements(count) + " Timestamp: " + count)
        ctx.collectWithTimestamp(elements(count), count)
        count += 1
      }
    }
    ctx.getCheckpointLock.synchronized {
      println("Emitting Watermark: " + (count / 2))
      ctx.emitWatermark(new Watermark(count / 2 + 1))
    }
    ctx.getCheckpointLock.synchronized {
      Thread.sleep(5000)
      println("Emitting Watermark: " + 5)
      ctx.emitWatermark(new Watermark(5))
    }
    while (isRunning) {
      ctx.getCheckpointLock.synchronized {
        Thread.sleep(100)
      }
    }
  }

  override def cancel(): Unit = isRunning = false

  override def snapshotState(
      checkpointId: Long,
      checkpointTimestamp: Long)
    : Integer = count

  override def restoreState(state: Integer): Unit = count = state
}
