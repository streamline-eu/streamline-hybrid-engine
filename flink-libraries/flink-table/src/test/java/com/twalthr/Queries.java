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

package com.twalthr;


import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.Trigger;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import scala.Option;

public class Queries {

	public static Logger LOG = LoggerFactory.getLogger(Queries.class);

	public static void main(String[] args) throws Exception {

		final String query;
		final int maxParallelism;
		final int parallelism;
		final boolean isRowtime;
		final boolean useStatistics;
		final int trigger;
		final long triggerPeriod;
		final boolean isHeap;
		final boolean async;
		final boolean inc;
		final String checkpointPath;
		final String inPath;
		final String outPath;
		final int days;
		final double dataFactor;
		final int seconds;
		final int nodes;
		final int edges;
		final boolean skewed;
		if (args.length == 0) {
			query = "simple";
			maxParallelism = 4;
			parallelism = 4;
			isRowtime = true;
			useStatistics = true;
			trigger = 1;
			triggerPeriod = 0L;
			isHeap = true;
			async = false;
			inc = false;
			checkpointPath = "file:///Users/twalthr/tmp/checkpoints";
			inPath = "/Users/twalthr/flink/data/mt/S0001/prepared";
			outPath = "/Users/twalthr/flink/data/mt/S0001/result";
			days = 10;
			dataFactor = 0.0001;
			seconds = 2;
			nodes = 4097; // estimated
			edges = 100000;
			skewed = false;
		} else {
			query = args[0];
			maxParallelism = Integer.parseInt(args[1]);
			parallelism = Integer.parseInt(args[2]);
			isRowtime = Boolean.parseBoolean(args[3]);
			useStatistics = Boolean.parseBoolean(args[4]);
			trigger = Integer.parseInt(args[5]);
			triggerPeriod = Long.parseLong(args[6]);
			isHeap = Boolean.parseBoolean(args[7]);
			async = Boolean.parseBoolean(args[8]);
			inc = Boolean.parseBoolean(args[9]);
			checkpointPath = args[10];
			inPath = args[11];
			outPath = args[12];
			days = Integer.parseInt(args[13]);
			dataFactor = Double.parseDouble(args[14]);
			seconds = Integer.parseInt(args[15]);
			nodes = Integer.parseInt(args[16]);
			edges = Integer.parseInt(args[17]);
			skewed = Boolean.parseBoolean(args[18]);
		}

		run(query, maxParallelism, parallelism, isRowtime, useStatistics, trigger, triggerPeriod,
			isHeap, async, inc, checkpointPath, inPath, outPath, days, dataFactor, seconds, nodes, edges,
			skewed);
	}

	public static void run(
			String query, int maxParallelism, int parallelism, boolean isRowtime, boolean useStatistics,
			int trigger, long triggerPeriod, boolean isHeap, boolean async, boolean inc, String checkpointPath,
			String inPath, String outPath, int days, double dataFactor, int seconds, int nodes, int edges, boolean skewed) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		if (isRowtime) {
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		}
		env.setParallelism(parallelism);
		env.setMaxParallelism(maxParallelism);
		if (isHeap) {
			env.setStateBackend(new MemoryStateBackend(async));
		} else {
			env.setStateBackend(new RocksDBStateBackend(checkpointPath, inc));
		}
		final TableConfig config = TableConfig.DEFAULT();
		final StreamTableEnvironment tenv = new StreamTableEnvironment(env, config);

		switch (query) {
			case "simple":
				runSimple(tenv, inPath, outPath, days, isRowtime, useStatistics, trigger, triggerPeriod, dataFactor, skewed);
				break;

			case "cyclic":
				runCyclic(tenv, inPath, outPath, seconds, isRowtime, useStatistics, trigger, triggerPeriod, nodes, edges, skewed);
				break;

			case "many":
				runMany(tenv, inPath, outPath, days, isRowtime, useStatistics, trigger, triggerPeriod, dataFactor, skewed);
				break;

			default:
				throw new IllegalArgumentException();
		}

		env.execute();
	}

	private static void runCyclic(StreamTableEnvironment tenv, String inPath, String outPath,
			int seconds, boolean isRowtime, boolean useStatistics, int trigger, long triggerPeriod, int nodes, int edges,
			boolean skewed) {

		final CustomCsvTableSource stackoverflow = CustomCsvTableSource.builder()
				.path(inPath + "/stackoverflow")
				.fieldDelimiter("|")
				.field("ts", Types.LONG())
				.field("u1", Types.INT())
				.field("u2", Types.INT())
				.build();
		stackoverflow.setOutOfOrder(seconds, TimeUnit.SECONDS);
		stackoverflow.setTimePrefix("");

		if (useStatistics && !skewed) {
			Map<String, ColumnStats> columnStats = new HashMap<>();
			// number of distinct values = nodes
			columnStats.put("u1", new ColumnStats((long) nodes, null, null, null, null, null, false));
			columnStats.put("u2", new ColumnStats((long) nodes, null, null, null, null, null, false));
			// number of rows = edges
			TableStats stats = new TableStats((long) edges, columnStats);
			tenv.registerTableSource("so", stackoverflow, stats);
		} else if (useStatistics) { // skewed

			// no non-skewed key, might not work

			Map<String, ColumnStats> columnStats = new HashMap<>();
			// number of distinct values = nodes
			columnStats.put("u1", new ColumnStats((long) nodes, null, null, null, null, null, true)); // skewed
			columnStats.put("u2", new ColumnStats((long) nodes, null, null, null, null, null, true)); // skewed
			// number of rows = edges
			TableStats stats = new TableStats((long) edges, columnStats);
			tenv.registerTableSource("so", stackoverflow, stats);
		} else {
			tenv.registerTableSource("so", stackoverflow);
		}

		final Table t;
		if (isRowtime) {
			t = tenv.sql(
					// 4-Clique (x, y, z, w)
					"SELECT R.u1, R.u2, Q.u1, Q.u2" +
					"FROM so AS R, so AS S, so AS T, so AS U, so AS V, so AS Q " +
					"WHERE " +
					// R(x,y), S(y,z), T(x,z), U(x,w), V(y,w), Q(z,w)
					"  R.u1 = T.u1 AND U.u1 = R.u1 AND " + // x
					"  R.u2 = S.u1 AND V.u1 = R.u2 AND " + // y
					"  S.u2 = T.u2 AND Q.u1 = S.u2 AND " + // z
					"  U.u2 = V.u2 AND Q.u2 = U.u2 AND " + // w
					"  JOINED_TIME(R.rowtime, S.rowtime, T.rowtime, U.rowtime, V.rowtime, Q.rowtime)");
		} else {
			t = tenv.sql(
					// 4-Clique (x, y, z, w)
					"SELECT R.u1, R.u2, Q.u1, Q.u2" +
					"FROM so AS R, so AS S, so AS T, so AS U, so AS V, so AS Q " +
					"WHERE " +
					// R(x,y), S(y,z), T(x,z), U(x,w), V(y,w), Q(z,w)
					"  R.u1 = T.u1 AND U.u1 = R.u1 AND " + // x
					"  R.u2 = S.u1 AND V.u1 = R.u2 AND " + // y
					"  S.u2 = T.u2 AND Q.u1 = S.u2 AND " + // z
					"  U.u2 = V.u2 AND Q.u2 = U.u2 AND " + // w
					"  JOINED_TIME(R.proctime, S.proctime, T.proctime, U.proctime, V.proctime, Q.proctime)");
		}
		final StreamQueryConfig conf;
		if (trigger == 0) {
			conf = tenv.queryConfig().withTrigger(Trigger.STREAM_TRIGGER());
		} else if (trigger == 1) {
			conf = tenv.queryConfig().withTrigger(Trigger.WATERMARK_TRIGGER());
		} else if (trigger == 2) {
			conf = tenv.queryConfig().withTrigger(Trigger.PERIODIC_TRIGGER(), triggerPeriod);
		} else {
			throw new IllegalArgumentException("Invalid trigger.");
		}

		Option<String> del = Option.<String>apply("|");
		Option<Object> numFile = Option.<Object>apply(null);
		Option<FileSystem.WriteMode> mode = Option.<FileSystem.WriteMode>apply(FileSystem.WriteMode.OVERWRITE);

		t.writeToSink(new CsvTableSink(
			outPath + "/cyclic_result",
			del,
			numFile,
			mode), conf);
	}

	public static void runSimple(StreamTableEnvironment tenv, String inPath, String outPath,
			int days, boolean isRowtime, boolean useStatistics, int trigger, long triggerPeriod, double dataFactor,
			boolean skewed) {

		final CustomCsvTableSource customer = CustomCsvTableSource.builder()
				.path(inPath + "/customer")
				.fieldDelimiter("|")
				.field("c_ts", Types.LONG())
				.field("c_custkey", Types.INT())
				.field("c_name", Types.STRING())
				.field("c_address", Types.STRING())
				.field("c_nationkey", Types.INT())
				.field("c_phone", Types.STRING())
				.field("c_acctbal", Types.DOUBLE())
				.field("c_mktsegment", Types.STRING())
				.field("c_comment", Types.STRING())
				.build();
		customer.setRowtime(isRowtime);
		customer.setOutOfOrder(days, TimeUnit.DAYS);
		customer.setTimePrefix("c_");

		final CustomCsvTableSource orders = CustomCsvTableSource.builder()
				.path(inPath + "/orders")
				.fieldDelimiter("|")
				.field("o_ts", Types.LONG())
				.field("o_orderkey", Types.INT())
				.field("o_custkey", Types.INT())
				.field("o_orderstatus", Types.STRING())
				.field("o_totalprice", Types.DOUBLE())
				.field("o_orderdate", Types.STRING())
				.field("o_orderpriority", Types.STRING())
				.field("o_clerk", Types.STRING())
				.field("o_shippriority", Types.INT())
				.field("o_comment", Types.STRING())
				.build();
		orders.setRowtime(isRowtime);
		orders.setOutOfOrder(days, TimeUnit.DAYS);
		orders.setTimePrefix("o_");

		if (useStatistics && !skewed) {
			{
				// SF * 150,000 rows in CUSTOMER table
				Map<String, ColumnStats> columnStats = new HashMap<>();
				final long rowCount = (long) (dataFactor * 150_000.0);
				columnStats.put("c_custkey", new ColumnStats(rowCount, null, null, null, null, null, false));
				TableStats stats = new TableStats(rowCount, columnStats);
				tenv.registerTableSource("customer", customer, stats);
			}

			{
				// for each row in the CUSTOMER table, ten rows in the ORDERS table
				Map<String, ColumnStats> columnStats = new HashMap<>();
				final long rowCount = (long) (dataFactor * 1_500_000.0);
				// orders are not present for all customers; every third customer is not assigned any order
				final long ndv = (long) (dataFactor * 150_000.0 * (2.0/3.0));
				columnStats.put("o_custkey", new ColumnStats(ndv, null, null, null, null, null, false));
				TableStats stats = new TableStats(rowCount, columnStats);
				tenv.registerTableSource("orders", orders, stats);
			}
		} else if (useStatistics) { // skewed

			// no non-skewed key, might not work

			{
				// SF * 150,000 rows in CUSTOMER table
				Map<String, ColumnStats> columnStats = new HashMap<>();
				final long rowCount = (long) (dataFactor * 150_000.0);
				columnStats.put("c_custkey", new ColumnStats(rowCount, null, null, null, null, null, false));
				TableStats stats = new TableStats(rowCount, columnStats);
				tenv.registerTableSource("customer", customer, stats);
			}

			{
				// for each row in the CUSTOMER table, ten rows in the ORDERS table
				Map<String, ColumnStats> columnStats = new HashMap<>();
				final long rowCount = (long) (dataFactor * 1_500_000.0);
				// orders are not present for all customers; every third customer is not assigned any order
				// despite skew, with large SF all values should be present, but we remove 1/3
				final long ndv_custkey = (long) (dataFactor * 150_000.0 * (2.0/3.0) * (2.0/3.0));
				columnStats.put("o_custkey", new ColumnStats(ndv_custkey, null, null, null, null, null, true)); // skewed
				// no non-skewed key, might not work
				TableStats stats = new TableStats(rowCount, columnStats);
				tenv.registerTableSource("orders", orders, stats);
			}
		} else {
			tenv.registerTableSource("customer", customer);
			tenv.registerTableSource("orders", orders);
		}

		final Table t;
		if (isRowtime) {
			t = tenv.sql(
				"SELECT c_ts, c_custkey, c_name, o_ts, o_orderkey, o_orderdate, o_orderstatus " +
				"FROM customer, orders " +
				"WHERE c_custkey = o_custkey AND JOINED_TIME(c_rowtime, o_rowtime)");
		} else {
			t = tenv.sql(
				"SELECT c_ts, c_custkey, c_name, o_ts, o_orderkey, o_orderdate, o_orderstatus " +
				"FROM customer, orders " +
				"WHERE c_custkey = o_custkey AND JOINED_TIME(c_proctime, o_proctime)");
		}
		final StreamQueryConfig conf;
		if (trigger == 0) {
			conf = tenv.queryConfig().withTrigger(Trigger.STREAM_TRIGGER());
		} else if (trigger == 1) {
			conf = tenv.queryConfig().withTrigger(Trigger.WATERMARK_TRIGGER());
		} else if (trigger == 2) {
			conf = tenv.queryConfig().withTrigger(Trigger.PERIODIC_TRIGGER(), triggerPeriod);
		} else {
			throw new IllegalArgumentException("Invalid trigger.");
		}

		Option<String> del = Option.<String>apply("|");
		Option<Object> numFile = Option.<Object>apply(null);
		Option<FileSystem.WriteMode> mode = Option.<FileSystem.WriteMode>apply(FileSystem.WriteMode.OVERWRITE);

		t.writeToSink(new CsvTableSink(
			outPath + "/simple_result",
			del,
			numFile,
			mode), conf);
	}

	public static void runMany(StreamTableEnvironment tenv, String inPath, String outPath,
			int days, boolean isRowtime, boolean useStatistics, int trigger, long triggerPeriod, double dataFactor,
			boolean skewed) {

		final CustomCsvTableSource customer = CustomCsvTableSource.builder()
				.path(inPath + "/customer")
				.fieldDelimiter("|")
				.field("c_ts", Types.LONG())
				.field("c_custkey", Types.INT())
				.field("c_name", Types.STRING())
				.field("c_address", Types.STRING())
				.field("c_nationkey", Types.INT())
				.field("c_phone", Types.STRING())
				.field("c_acctbal", Types.DOUBLE())
				.field("c_mktsegment", Types.STRING())
				.field("c_comment", Types.STRING())
				.build();
		customer.setRowtime(isRowtime);
		customer.setOutOfOrder(days, TimeUnit.DAYS);
		customer.setTimePrefix("c_");

		final CustomCsvTableSource orders = CustomCsvTableSource.builder()
				.path(inPath + "/orders")
				.fieldDelimiter("|")
				.field("o_ts", Types.LONG())
				.field("o_orderkey", Types.INT())
				.field("o_custkey", Types.INT())
				.field("o_orderstatus", Types.STRING())
				.field("o_totalprice", Types.DOUBLE())
				.field("o_orderdate", Types.STRING())
				.field("o_orderpriority", Types.STRING())
				.field("o_clerk", Types.STRING())
				.field("o_shippriority", Types.INT())
				.field("o_comment", Types.STRING())
				.build();
		orders.setRowtime(isRowtime);
		orders.setOutOfOrder(days, TimeUnit.DAYS);
		orders.setTimePrefix("o_");

		final CustomCsvTableSource lineitem = CustomCsvTableSource.builder()
				.path(inPath + "/lineitem")
				.fieldDelimiter("|")
				.field("l_ts", Types.LONG())
				.field("l_orderkey", Types.INT())
				.field("l_partkey", Types.INT())
				.field("l_suppkey", Types.INT())
				.field("l_linenumber", Types.INT())
				.field("l_quantity", Types.INT())
				.field("l_extendedprice", Types.DOUBLE())
				.field("l_discount", Types.DOUBLE())
				.field("l_tax", Types.DOUBLE())
				.field("l_returnflag", Types.STRING())
				.field("l_linestatus", Types.STRING())
				.field("l_shipdate", Types.STRING())
				.field("l_commitdate", Types.STRING())
				.field("l_receiptdate", Types.STRING())
				.field("l_shipinstruct", Types.STRING())
				.field("l_shipmode", Types.STRING())
				.field("l_comment", Types.STRING())
				.build();
		lineitem.setRowtime(isRowtime);
		lineitem.setOutOfOrder(days, TimeUnit.DAYS);
		lineitem.setTimePrefix("l_");

		final CustomCsvTableSource supplier = CustomCsvTableSource.builder()
				.path(inPath + "/supplier")
				.fieldDelimiter("|")
				.field("s_ts", Types.LONG())
				.field("s_suppkey", Types.INT())
				.field("s_name", Types.STRING())
				.field("s_address", Types.STRING())
				.field("s_nationkey", Types.INT())
				.field("s_phone", Types.STRING())
				.field("s_acctbal", Types.DOUBLE())
				.field("s_comment", Types.STRING())
				.build();
		supplier.setRowtime(isRowtime);
		supplier.setOutOfOrder(-1, null);
		supplier.setTimePrefix("s_");

		final CustomCsvTableSource nation = CustomCsvTableSource.builder()
				.path(inPath + "/nation")
				.fieldDelimiter("|")
				.field("n_ts", Types.LONG())
				.field("n_nationkey", Types.INT())
				.field("n_name", Types.STRING())
				.field("n_regionkey", Types.INT())
				.field("n_comment", Types.STRING())
				.build();
		nation.setRowtime(isRowtime);
		nation.setOutOfOrder(-1, null);
		nation.setTimePrefix("n_");

		final CustomCsvTableSource region = CustomCsvTableSource.builder()
				.path(inPath + "/region")
				.fieldDelimiter("|")
				.field("r_ts", Types.LONG())
				.field("r_regionkey", Types.INT())
				.field("r_name", Types.STRING())
				.field("r_comment", Types.STRING())
				.build();
		region.setRowtime(isRowtime);
		region.setOutOfOrder(-1, null);
		region.setTimePrefix("r_");

		if (useStatistics && !skewed) {
			{
				// SF * 150,000 rows in CUSTOMER table
				Map<String, ColumnStats> columnStats = new HashMap<>();
				final long rowCount = (long) (dataFactor * 150_000.0);
				columnStats.put("c_custkey", new ColumnStats(rowCount, null, null, null, null, null, false));
				// fixed cardinality: does not scale with SF
				columnStats.put("c_nationkey", new ColumnStats(25L, null, null, null, null, null, false));
				TableStats stats = new TableStats(rowCount, columnStats);
				tenv.registerTableSource("customer", customer, stats);
			}

			{
				// for each row in the CUSTOMER table, ten rows in the ORDERS table
				Map<String, ColumnStats> columnStats = new HashMap<>();
				final long rowCount = (long) (dataFactor * 1_500_000.0);
				// orders are not present for all customers; every third customer is not assigned any order
				final long ndv_custkey = (long) (dataFactor * 150_000.0 * (2.0/3.0));
				columnStats.put("o_custkey", new ColumnStats(ndv_custkey, null, null, null, null, null, false));
				columnStats.put("o_orderkey", new ColumnStats(rowCount, null, null, null, null, null, false));
				TableStats stats = new TableStats(rowCount, columnStats);
				tenv.registerTableSource("orders", orders, stats);
			}

			{
				// cardinality of the LINEITEM table is not a strict multiple of SF since
				// the number of lineitems in an order is chosen at random with an average of four
				Map<String, ColumnStats> columnStats = new HashMap<>();
				final long rowCount = (long) (dataFactor * 6_000_000.0);
				// orders are chosen at random with an average of four
				final long ndv_orderkey = (long) (dataFactor * 1_500_000.0);
				columnStats.put("l_orderkey", new ColumnStats(ndv_orderkey, null, null, null, null, null, false));
				// SF * 10,000 rows in the SUPPLIER table
				final long ndv_suppkey = (long) (dataFactor * 10_000.0);
				columnStats.put("l_suppkey", new ColumnStats(ndv_suppkey, null, null, null, null, null, false));
				TableStats stats = new TableStats(rowCount, columnStats);
				tenv.registerTableSource("lineitem", lineitem, stats);
			}

			{
				// SF * 10,000 rows in the SUPPLIER table
				Map<String, ColumnStats> columnStats = new HashMap<>();
				final long rowCount = (long) (dataFactor * 10_000.0);
				columnStats.put("s_suppkey", new ColumnStats(rowCount, null, null, null, null, null, false));
				// 25 rows in the NATION table
				columnStats.put("s_nationkey", new ColumnStats(25L, null, null, null, null, null, false));
				TableStats stats = new TableStats(rowCount, columnStats);
				tenv.registerTableSource("supplier", supplier, stats);
			}

			{
				// 25 rows in the NATION table
				Map<String, ColumnStats> columnStats = new HashMap<>();
				columnStats.put("n_nationkey", new ColumnStats(25L, null, null, null, null, null, false));
				// 5 rows in the REGION table
				columnStats.put("n_regionkey", new ColumnStats(5L, null, null, null, null, null, false));
				TableStats stats = new TableStats(25L, columnStats);
				tenv.registerTableSource("nation", nation, stats);
			}

			{
				// 5 rows in the REGION table
				Map<String, ColumnStats> columnStats = new HashMap<>();
				columnStats.put("r_regionkey", new ColumnStats(5L, null, null, null, null, null, false));
				TableStats stats = new TableStats(5L, columnStats);
				tenv.registerTableSource("region", region, stats);
			}
		} else if (useStatistics) { // skewed
			{
				// SF * 150,000 rows in CUSTOMER table
				Map<String, ColumnStats> columnStats = new HashMap<>();
				final long rowCount = (long) (dataFactor * 150_000.0);
				columnStats.put("c_custkey", new ColumnStats(rowCount, null, null, null, null, null, false));
				// fixed cardinality: does not scale with SF
				// despite skew, with large SF all values should be present, but we remove 1/3
				columnStats.put("c_nationkey", new ColumnStats((long) (25.0 * (2.0/3.0)), null, null, null, null, null, true)); // skewed
				TableStats stats = new TableStats(rowCount, columnStats);
				tenv.registerTableSource("customer", customer, stats);
			}

			{
				// for each row in the CUSTOMER table, ten rows in the ORDERS table
				Map<String, ColumnStats> columnStats = new HashMap<>();
				final long rowCount = (long) (dataFactor * 1_500_000.0);
				// orders are not present for all customers; every third customer is not assigned any order
				// despite skew, with large SF all values should be present, but we remove 1/3
				final long ndv_custkey = (long) (dataFactor * 150_000.0 * (2.0/3.0) * (2.0/3.0));
				columnStats.put("o_custkey", new ColumnStats(ndv_custkey, null, null, null, null, null, true)); // skewed
				columnStats.put("o_orderkey", new ColumnStats(rowCount, null, null, null, null, null, false));
				TableStats stats = new TableStats(rowCount, columnStats);
				tenv.registerTableSource("orders", orders, stats);
			}

			{
				// cardinality of the LINEITEM table is not a strict multiple of SF since
				// the number of lineitems in an order is chosen at random with an average of four
				Map<String, ColumnStats> columnStats = new HashMap<>();
				final long rowCount = (long) (dataFactor * 6_000_000.0);
				// orders are chosen at random with an average of four
				// despite skew, with large SF all values should be present, but we remove 1/3
				final long ndv_orderkey = (long) (dataFactor * 1_500_000.0 * (2.0/3.0));
				columnStats.put("l_orderkey", new ColumnStats(ndv_orderkey, null, null, null, null, null, true)); // skewed
				// SF * 10,000 rows in the SUPPLIER table
				final long ndv_suppkey = (long) (dataFactor * 10_000.0);
				columnStats.put("l_suppkey", new ColumnStats(ndv_suppkey, null, null, null, null, null, false));
				TableStats stats = new TableStats(rowCount, columnStats);
				tenv.registerTableSource("lineitem", lineitem, stats);
			}

			{
				// SF * 10,000 rows in the SUPPLIER table
				Map<String, ColumnStats> columnStats = new HashMap<>();
				final long rowCount = (long) (dataFactor * 10_000.0);
				columnStats.put("s_suppkey", new ColumnStats(rowCount, null, null, null, null, null, false));
				// 25 rows in the NATION table
				// despite skew, with large SF all values should be present, but we remove 1/3
				columnStats.put("s_nationkey", new ColumnStats((long) (25.0 * (2.0/3.0)), null, null, null, null, null, true)); // skewed
				TableStats stats = new TableStats(rowCount, columnStats);
				tenv.registerTableSource("supplier", supplier, stats);
			}

			{
				// 25 rows in the NATION table
				Map<String, ColumnStats> columnStats = new HashMap<>();
				columnStats.put("n_nationkey", new ColumnStats(25L, null, null, null, null, null, false));
				// 5 rows in the REGION table
				columnStats.put("n_regionkey", new ColumnStats(5L, null, null, null, null, null, false));
				TableStats stats = new TableStats(25L, columnStats);
				tenv.registerTableSource("nation", nation, stats);
			}

			{
				// 5 rows in the REGION table
				Map<String, ColumnStats> columnStats = new HashMap<>();
				columnStats.put("r_regionkey", new ColumnStats(5L, null, null, null, null, null, false));
				TableStats stats = new TableStats(5L, columnStats);
				tenv.registerTableSource("region", region, stats);
			}
		} else {
			tenv.registerTableSource("customer", customer);
			tenv.registerTableSource("orders", orders);
			tenv.registerTableSource("lineitem", lineitem);
			tenv.registerTableSource("supplier", supplier);
			tenv.registerTableSource("nation", nation);
			tenv.registerTableSource("region", region);
		}

		final Table t;
		if (isRowtime) {
			t = tenv.sql(
					"SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue " +
					"FROM customer, orders, lineitem, supplier, nation, region " +
					"WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND " +
					"  c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND " +
					"  r_name = 'ASIA' AND o_orderdate >= DATE '1994-01-01' AND o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR AND" +
					"  JOINED_TIME(c_rowtime, o_rowtime, l_rowtime, s_rowtime, n_rowtime, r_rowtime)" +
					"GROUP BY n_name "); // we skip sorting as it would limit the parallelism to 1
		} else {
//			t = tenv.sql(
//					"SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue " +
//					"FROM customer, orders, lineitem, supplier, nation, region " +
//					"WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND " +
//					"  c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND " +
//					"  r_name = 'ASIA' AND o_orderdate >= DATE '1994-01-01' AND o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR AND" +
//					"  JOINED_TIME(c_proctime, o_proctime, l_proctime, s_proctime, n_proctime, r_proctime)" +
//					"GROUP BY n_name"); // we skip sorting as it would limit the parallelism to 1

			t = tenv.sql(
					"SELECT n_name, l_extendedprice * (1 - l_discount) AS revenue " +
					"FROM customer, orders, lineitem, supplier, nation, region " +
					"WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND " +
					"  c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND " +
					"  r_name = 'ASIA' AND o_orderdate >= DATE '1994-01-01' AND o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR AND " +
					"  JOINED_TIME(c_proctime, o_proctime, l_proctime, s_proctime, n_proctime, r_proctime)"); // we skip sorting as it would limit the parallelism to 1
		}
		final StreamQueryConfig conf;
		if (trigger == 0) {
			conf = tenv.queryConfig().withTrigger(Trigger.STREAM_TRIGGER());
		} else if (trigger == 1) {
			conf = tenv.queryConfig().withTrigger(Trigger.WATERMARK_TRIGGER());
		} else if (trigger == 2) {
			conf = tenv.queryConfig().withTrigger(Trigger.PERIODIC_TRIGGER(), triggerPeriod);
		} else {
			throw new IllegalArgumentException("Invalid trigger.");
		}

		Option<String> del = Option.<String>apply("|");
		Option<Object> numFile = Option.<Object>apply(null);
		Option<FileSystem.WriteMode> mode = Option.<FileSystem.WriteMode>apply(FileSystem.WriteMode.OVERWRITE);

//		t.writeToSink(new CsvTableSink(
//			outPath + "/many_result",
//			del,
//			numFile,
//			mode), conf);
//		tenv.toRetractStream(t, Row.class);

		tenv.explain(t);
	}
}
