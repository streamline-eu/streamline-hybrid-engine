package com.twalthr;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import org.apache.calcite.runtime.SqlFunctions;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import scala.Option;

public class PreparationSimpleAndMany {

	public static void main(String[] args) throws Exception {
		final String inPath;
		final int days;
		final String outPath;
		if (args.length == 0) {
			inPath = "/Volumes/TTDISK/tpchskew/raw/32GB";
			days = 5;
//			outPath = "/Users/twalthr/flink/data/clusterdata/tpch/8GB/prepared";
			outPath = "/Volumes/TTDISK/tpchskew/prepared/32GB";
		} else {
			inPath = args[0];
			days = Integer.parseInt(args[1]);
			outPath = args[2];
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		BatchTableEnvironment tenv = new BatchTableEnvironment(env, TableConfig.DEFAULT());

		{
			final CsvTableSource customer = CsvTableSource.builder()
					.path(inPath + "/customer.tbl")
					.fieldDelimiter("|")
					.field("c_custkey", Types.INT())
					.field("c_name", Types.STRING())
					.field("c_address", Types.STRING())
					.field("c_nationkey", Types.INT())
					.field("c_phone", Types.STRING())
					.field("c_acctbal", Types.DOUBLE())
					.field("c_mktsegment", Types.STRING())
					.field("c_comment", Types.STRING())
					.build();

			tenv.registerTableSource("customer", customer);
		}

		{
			final TableSource<Row> orders = CsvTableSource.builder()
					.path(inPath + "/order.tbl")
					.fieldDelimiter("|")
					.field("o_orderkey", Types.INT())
					.field("o_custkey", Types.INT())
					.field("o_orderstatus", Types.STRING())
					.field("o_totalprice", Types.DOUBLE())
					.field("o_orderdate", Types.SQL_DATE())
					.field("o_orderpriority", Types.STRING())
					.field("o_clerk", Types.STRING())
					.field("o_shippriority", Types.INT())
					.field("o_comment", Types.STRING())
					.build();

			tenv.registerTableSource("orders", orders);
		}

		{
			final TableSource<Row> lineitem = CsvTableSource.builder()
					.path(inPath + "/lineitem.tbl")
					.fieldDelimiter("|")
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
					.field("l_shipdate", Types.SQL_DATE())
					.field("l_commitdate", Types.SQL_DATE())
					.field("l_receiptdate", Types.SQL_DATE())
					.field("l_shipinstruct", Types.STRING())
					.field("l_shipmode", Types.STRING())
					.field("l_comment", Types.STRING())
					.build();

			tenv.registerTableSource("lineitem", lineitem);
		}

		{
			final CsvTableSource supplier = CsvTableSource.builder()
					.path(inPath + "/supplier.tbl")
					.fieldDelimiter("|")
					.field("s_suppkey", Types.INT())
					.field("s_name", Types.STRING())
					.field("s_address", Types.STRING())
					.field("s_nationkey", Types.INT())
					.field("s_phone", Types.STRING())
					.field("s_acctbal", Types.DOUBLE())
					.field("s_comment", Types.STRING())
					.build();

			tenv.registerTableSource("supplier", supplier);
		}

		{
			final CsvTableSource nation = CsvTableSource.builder()
					.path(inPath + "/nation.tbl")
					.fieldDelimiter("|")
					.field("n_nationkey", Types.INT())
					.field("n_name", Types.STRING())
					.field("n_regionkey", Types.INT())
					.field("n_comment", Types.STRING())
					.build();

			tenv.registerTableSource("nation", nation);
		}

		{
			final CsvTableSource region = CsvTableSource.builder()
					.path(inPath + "/region.tbl")
					.fieldDelimiter("|")
					.field("r_regionkey", Types.INT())
					.field("r_name", Types.STRING())
					.field("r_comment", Types.STRING())
					.build();

			tenv.registerTableSource("region", region);
		}

		tenv.registerFunction("convTs", new ConvertTimestampFunction());
		tenv.registerFunction("convTs2", new ConvertTimstamp2Function());

		// write out of order customers
		{
			Table tc = tenv.sql(
					"SELECT MIN(convTs(CAST(o_orderdate AS TIMESTAMP))) AS c_ts, c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment " +
					"FROM customer LEFT JOIN orders ON c_custkey = o_custkey " +
					"GROUP BY c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment " +
					"ORDER BY c_ts");

			DataSet<Row> orderedCustomersDataSet = tenv.toDataSet(tc, Row.class);

			Table orderedCustomersTable = tenv.fromDataSet(
					orderedCustomersDataSet.mapPartition(new OutOfOrderFunction<>(days)),
					"c_ts, c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment");

			writeToSink(orderedCustomersTable, outPath + "/customer");
		}

		// write out of order orders
		{
			Table to = tenv.sql(
					"SELECT MAX(convTs2(CAST(l_shipdate AS TIMESTAMP), CAST(l_commitdate AS TIMESTAMP), CAST(l_receiptdate AS TIMESTAMP), CAST(o_orderdate AS TIMESTAMP))) AS o_ts, " +
					"  o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment " +
					"FROM orders LEFT JOIN lineitem ON o_orderkey = l_orderkey " +
					"GROUP BY o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment " +
					"ORDER BY o_ts");

			DataSet<Row> orderedOrdersDataSet = tenv.toDataSet(to, Row.class);

			Table orderedOrdersTable = tenv.fromDataSet(
					orderedOrdersDataSet.mapPartition(new OutOfOrderFunction<>(days)),
					"o_ts, o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment");

			writeToSink(orderedOrdersTable, outPath + "/orders");
		}

		// write out of order lineitems
//		{
//			Table tl = tenv.sql(
//					"SELECT convTs2(CAST(l_shipdate AS TIMESTAMP), CAST(l_commitdate AS TIMESTAMP), CAST(l_receiptdate AS TIMESTAMP), CAST(o_orderdate AS TIMESTAMP)) AS l_ts, " +
//					"  l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment " +
//					"FROM lineitem LEFT JOIN orders ON l_orderkey = o_orderkey " +
//					"ORDER BY l_ts");
//
//			DataSet<Row> orderedLineitemDataSet = tenv.toDataSet(tl, Row.class);
//
//			Table orderedLineitemsTable = tenv.fromDataSet(
//					orderedLineitemDataSet.mapPartition(new OutOfOrderFunction<>(days)),
//					"l_ts, l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, " +
//					"l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment");
//
//			writeToSink(orderedLineitemsTable, outPath + "/lineitem");
//		}

		// write suppliers
		{
			Table ts = tenv.sql(
					"SELECT 694180800000 AS s_ts, s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment " +
					"FROM supplier");

			writeToSink(ts, outPath + "/supplier");
		}

		// write nation
		{
			Table tn = tenv.sql(
					"SELECT 694180800000 AS n_ts, n_nationkey, n_name, n_regionkey, n_comment " +
					"FROM nation");

			writeToSink(tn, outPath + "/nation");
		}

		// write region
		{
			Table tr = tenv.sql(
					"SELECT 694180800000 AS r_ts, r_regionkey, r_name, r_comment " +
					"FROM region");

			writeToSink(tr, outPath + "/region");
		}

		env.execute();
	}

	public static void writeToSink(Table t, String path) {
		Option<String> del = Option.<String>apply("|");
		Option<Object> numFile = Option.<Object>apply(null);
		Option<FileSystem.WriteMode> mode = Option.<FileSystem.WriteMode>apply(FileSystem.WriteMode.OVERWRITE);

		t.writeToSink(new CsvTableSink(
			path,
			del,
			numFile,
			mode));
	}

	public static class ConvertTimestampFunction extends ScalarFunction {
		@SuppressWarnings("unused")
		public long eval(long ts) {
			if (ts < 0) {
				final long from = SqlFunctions.toLong(Timestamp.valueOf("1992-01-01 00:00:00.000"));
				final long to = SqlFunctions.toLong(Timestamp.valueOf("1998-12-31 23:59:59.999"));
				return ThreadLocalRandom.current().nextLong(from, to);
			}
			return ts;
		}
	}

	public static class ConvertTimstamp2Function extends ScalarFunction {
		@SuppressWarnings("unused")
		public long eval(long ts1, long ts2, long ts3, long fallbackTs) {
			return Math.max(ts1, Math.max(ts2, Math.max(ts3, fallbackTs)));
		}
	}

	public static class OutOfOrderFunction<T> extends RichMapPartitionFunction<T, T> {

		private int days;

		List<T> values;

		public OutOfOrderFunction(int days) {
			this.days = days;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			values = new ArrayList<>();
		}

		@Override
		public void mapPartition(Iterable<T> iterable, Collector<T> collector) throws Exception {
			final Iterator<T> iter = iterable.iterator();

			long last_ts = Long.MIN_VALUE;
			while (iter.hasNext()) {
				final T orig = iter.next();
				final long next_ts = ((Long) ((Row) orig).getField(0));
				if (last_ts == Long.MIN_VALUE) {
					last_ts = next_ts;
				} else if (last_ts < next_ts - TimeUnit.DAYS.toMillis(days)) {
					last_ts = next_ts;
					// shuffle
					Collections.shuffle(values);
					// emit
					for (T value : values) {
						collector.collect(value);
					}
					// empty
					values.clear();
				}
				values.add(orig);
			}

			// shuffle
			Collections.shuffle(values);
			// emit
			for (T value : values) {
				collector.collect(value);
			}
			// empty
			values.clear();
		}
	}
}
