package com.twalthr;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Iterator;

import scala.Option;

public class ValidationSimpleAndMany {

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

		result.writeAsText(validatePath + "/simple", FileSystem.WriteMode.OVERWRITE);
	}

	private static void validateMany(BatchTableEnvironment tenv, String inPath, String outPath, String validatePath) {

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
