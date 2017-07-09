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
package org.apache.flink.table.examples.scala

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

/**
  * Simple example for demonstrating the use of SQL on a Stream Table.
  *
  * This example shows how to:
  *  - Convert DataStreams to Tables
  *  - Register a Table under a name
  *  - Run a StreamSQL query on the registered Table
  *
  */
object StreamSQLExample {

  // *************************************************************************
  //     PROGRAM
  // *************************************************************************

  def main(args: Array[String]): Unit = {

    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val orders: DataStream[Order] = env.fromCollection(Seq(
      Order(1L, 1),
      Order(1L, 2),
      Order(3L, 1)))

    val rates: DataStream[Rate] = env.fromCollection(Seq(
      Rate(1, 3.226266),
      Rate(2, 2.17272),
      Rate(3, 0)))

    // Orders(rowtime, amount, currency)
    // Rates(rowtime, currency, rate)
    tEnv.registerDataStream("Orders", orders, 'amount, 'currency, 'proctime.proctime)
    tEnv.registerDataStream("Rates", rates, 'currency, 'rate, 'proctime.proctime)

    // union the two tables
    val result = tEnv.sql(
      "SELECT Orders.amount * Rates.rate AS totalAmount " +
        "FROM Orders, Rates " +
        "WHERE Orders.currency = Rates.currency AND Rates.proctime = (" +
          "SELECT MAX(r.proctime) FROM Rates AS r WHERE r.currency = Orders.currency AND r.proctime <= Orders.proctime" +
        ")")

    result.toAppendStream[Order].print()

    env.execute()
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class Order(amount: Long, currency: Int)

  case class Rate(currency: Int, rate: Double)

}
