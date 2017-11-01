package com.twalthr;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Iterator;

import scala.Option;

public class Validation {

	public static void main(String[] args) throws Exception {
		final String query;
		final String inPath;
		final String outPath;
		final String validatePath;
		if (args.length == 0) {
			query = "simple";
			inPath = "/Users/twalthr/flink/data/mt/S0001/prepared";
			outPath = "/Users/twalthr/flink/data/mt/S0001/result";
			validatePath = "/Users/twalthr/flink/data/mt/S0001/validate";
		} else {
			query = args[0];
			inPath = args[1];
			outPath = args[2];
			validatePath = args[3];
		}

		run(query, inPath, outPath, validatePath);
	}

	public static void run(String query, String inPath, String outPath, String validatePath) throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		final BatchTableEnvironment tenv = TableEnvironment.getTableEnvironment(env);

		switch (query) {
			case "simple":
				validateSimple(tenv, inPath, outPath, validatePath);
				break;

			case "many":
				validateMany(tenv, inPath, outPath, validatePath);
				break;

			case "cyclic":
				validateCyclic(tenv, inPath, outPath, validatePath);
				break;

			default:
				throw new IllegalArgumentException();
		}

		env.execute();
	}

	private static void validateSimple(BatchTableEnvironment tenv, String inPath, String outPath, String validatePath) {

		// expected
		final CsvTableSource customer = CsvTableSource.builder()
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
		tenv.registerTableSource("outOfOrderCustomer", customer);

		final CsvTableSource orders = CsvTableSource.builder()
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
		tenv.registerTableSource("outOfOrderOrders", orders);

		final Table customerTable = tenv.sql("SELECT * FROM outOfOrderCustomer ORDER BY c_ts");
		tenv.registerTable("customer", customerTable);
		final Table ordersTable = tenv.sql("SELECT * FROM outOfOrderOrders ORDER BY o_ts");
		tenv.registerTable("orders", ordersTable);

		final Table expected = tenv.sql(
				"SELECT c_ts, c_custkey, c_name, o_ts, o_orderkey, o_orderdate, o_orderstatus " +
				"FROM customer, orders " +
				"WHERE c_custkey = o_custkey");
		writeToSink(expected, validatePath + "/simple_expected");

		// actual
		final CsvTableSource actualSource = CsvTableSource.builder()
				.path(outPath + "/simple_result")
				.fieldDelimiter("|")
				.field("c_ts", Types.LONG())
				.field("c_custkey", Types.INT())
				.field("c_name", Types.STRING())
				.field("o_ts", Types.LONG())
				.field("o_orderkey", Types.INT())
				.field("o_orderdate", Types.STRING())
				.field("o_orderstatus", Types.STRING())
				.build();
		tenv.registerTableSource("actual", actualSource);
		final Table actual = tenv.scan("actual");

		// compare
		compare(tenv, expected, actual, validatePath);
	}

	private static void validateMany(BatchTableEnvironment tenv, String inPath, String outPath, String validatePath) {

		// expected
		final CsvTableSource customer = CsvTableSource.builder()
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

		final CsvTableSource orders = CsvTableSource.builder()
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

		final CsvTableSource lineitem = CsvTableSource.builder()
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

		final CsvTableSource supplier = CsvTableSource.builder()
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

		final CsvTableSource nation = CsvTableSource.builder()
				.path(inPath + "/nation")
				.fieldDelimiter("|")
				.field("n_ts", Types.LONG())
				.field("n_nationkey", Types.INT())
				.field("n_name", Types.STRING())
				.field("n_regionkey", Types.INT())
				.field("n_comment", Types.STRING())
				.build();

		final CsvTableSource region = CsvTableSource.builder()
				.path(inPath + "/region")
				.fieldDelimiter("|")
				.field("r_ts", Types.LONG())
				.field("r_regionkey", Types.INT())
				.field("r_name", Types.STRING())
				.field("r_comment", Types.STRING())
				.build();

		tenv.registerTableSource("outOfOrderCustomer", customer);
		tenv.registerTableSource("outOfOrderOrders", orders);
		tenv.registerTableSource("outOfOrderLineitem", lineitem);
		tenv.registerTableSource("outOfOrderSupplier", supplier);
		tenv.registerTableSource("outOfOrderNation", nation);
		tenv.registerTableSource("outOfOrderRegion", region);

		final Table customerTable = tenv.sql("SELECT * FROM outOfOrderCustomer ORDER BY c_ts");
		tenv.registerTable("customer", customerTable);
		final Table ordersTable = tenv.sql("SELECT * FROM outOfOrderOrders ORDER BY o_ts");
		tenv.registerTable("orders", ordersTable);
		final Table lineitemTable = tenv.sql("SELECT * FROM outOfOrderLineitem ORDER BY l_ts");
		tenv.registerTable("lineitem", lineitemTable);
		final Table supplierTable = tenv.sql("SELECT * FROM outOfOrderSupplier ORDER BY s_ts");
		tenv.registerTable("supplier", supplierTable);
		final Table nationTable = tenv.sql("SELECT * FROM outOfOrderNation ORDER BY n_ts");
		tenv.registerTable("nation", nationTable);
		final Table regionTable = tenv.sql("SELECT * FROM outOfOrderRegion ORDER BY r_ts");
		tenv.registerTable("region", regionTable);

		final Table expected = tenv.sql(
			"SELECT n_name, l_extendedprice * (1 - l_discount) AS revenue " +
			"FROM customer, orders, lineitem, supplier, nation, region " +
			"WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND " +
			"  c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND " +
			"  r_name = 'ASIA' AND o_orderdate >= DATE '1994-01-01' AND o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR");
		writeToSink(expected, validatePath + "/many_expected");

		// actual
		final CsvTableSource actualSource = CsvTableSource.builder()
				.path(outPath + "/many_result")
				.fieldDelimiter("|")
				.field("n_name", Types.STRING())
				.field("revenue", Types.DOUBLE())
				.build();
		tenv.registerTableSource("actual", actualSource);
		final Table actual = tenv.scan("actual");

		// compare
		compare(tenv, expected, actual, validatePath);
	}

	private static void validateCyclic(BatchTableEnvironment tenv, String inPath, String outPath, String validatePath) {

		// expected
		final CsvTableSource edges = CsvTableSource.builder()
				.path(inPath + "/edges")
				.fieldDelimiter("|")
				.field("ts", Types.LONG())
				.field("src", Types.INT())
				.field("dst", Types.INT())
				.build();
		tenv.registerTableSource("outOfOrderEdges", edges);

		final Table edgesTable = tenv.sql("SELECT * FROM outOfOrderEdges ORDER BY ts");
		tenv.registerTable("edges", edgesTable);

		final Table expected = tenv.sql(
			"SELECT R.src, R.dst, T.src " +
			"FROM edges AS R, edges AS S, edges AS T " +
			"WHERE R.dst = S.src AND S.dst = T.src AND T.dst = R.src");
		writeToSink(expected, validatePath + "/cyclic_expected");

//		// actual
//		final CsvTableSource actualSource = CsvTableSource.builder()
//				.path(outPath + "/cyclic_result")
//				.fieldDelimiter("|")
//				.field("Rsrc", Types.INT())
//				.field("Rdst", Types.INT())
//				.field("Tsrc", Types.INT())
//				.build();
//		tenv.registerTableSource("actual", actualSource);
//		final Table actual = tenv.scan("actual");
//
//		// compare
//		compare(tenv, expected, actual, validatePath);
	}

	public static void compare(BatchTableEnvironment tenv, Table expected, Table actual, String validatePath) {
				final DataSet<Tuple2<Integer, String>> expectedDs = tenv
			.toDataSet(expected, Row.class)
			.map(new ToStringMapFunction<>())
			.sortPartition("*", Order.ASCENDING)
			.mapPartition(new NumberingMapPartionFunction());

		final DataSet<Tuple2<Integer, String>> actualDs = tenv
			.toDataSet(actual, Row.class)
			.map(new ToStringMapFunction<>())
			.sortPartition("*", Order.ASCENDING)
			.mapPartition(new NumberingMapPartionFunction());

		final DataSet<String> result = expectedDs
				.coGroup(actualDs)
				.where(0)
				.equalTo(0)
				.with(new CoGroupFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, String>() {

			@Override
			public void coGroup(
					Iterable<Tuple2<Integer, String>> exp,
					Iterable<Tuple2<Integer, String>> act,
					Collector<String> out) throws Exception {

				final Iterator<Tuple2<Integer, String>> expIter = exp.iterator();
				final Iterator<Tuple2<Integer, String>> actIter = act.iterator();
				while (expIter.hasNext() && actIter.hasNext()) {
					final Tuple2<Integer, String> expNext = expIter.next();
					final Tuple2<Integer, String> actNext = actIter.next();
					if (!expNext.equals(actNext)) {
						throw new IllegalArgumentException("Expected: " + expNext + " | Actual: " + actNext);
					}
				}
				if (expIter.hasNext()) {
					final Tuple2<Integer, String> expNext = expIter.next();
					throw new IllegalArgumentException("Missing: " + expNext);
				}
				if (actIter.hasNext()) {
					final Tuple2<Integer, String> actNext = actIter.next();
					throw new IllegalArgumentException("Not expected: " + actNext);
				}
			}
		});

		result.writeAsText(validatePath + "/valout", FileSystem.WriteMode.OVERWRITE);
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

	public static class ToStringMapFunction<T> implements MapFunction<T, String> {

		@Override
		public String map(T value) throws Exception {
			return value.toString().replace(',', '|');
		}
	}

	public static class NumberingMapPartionFunction implements MapPartitionFunction<String, Tuple2<Integer, String>> {

		private int idx = 0;

		@Override
		public void mapPartition(Iterable<String> iterable, Collector<Tuple2<Integer, String>> out) throws Exception {
			for (String value : iterable) {
				out.collect(new Tuple2<>(idx++, value));
			}
		}
	}
}
