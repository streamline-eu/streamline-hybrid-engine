package com.twalthr;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Iterator;

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
		tenv.toDataSet(expected, Row.class).writeAsText(validatePath + "/simple_expected");

		// actual
		final CsvTableSource actualSource = CsvTableSource.builder()
				.path(outPath + "/simple")
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
		final DataSet<Tuple2<Integer, Row>> expectedDs = tenv.toDataSet(expected, Row.class)
				.mapPartition(new NumberingMapPartionFunction<>());
		final DataSet<Tuple2<Integer, Row>> actualDs = tenv.toDataSet(actual, Row.class)
				.mapPartition(new NumberingMapPartionFunction<>());

		final DataSet<String> result = expectedDs
				.coGroup(actualDs)
				.where(0)
				.equalTo(0)
				.with(new CoGroupFunction<Tuple2<Integer,Row>, Tuple2<Integer,Row>, String>() {

			@Override
			public void coGroup(
					Iterable<Tuple2<Integer, Row>> exp,
					Iterable<Tuple2<Integer, Row>> act,
					Collector<String> out) throws Exception {

				final Iterator<Tuple2<Integer, Row>> expIter = exp.iterator();
				final Iterator<Tuple2<Integer, Row>> actIter = act.iterator();
				while (expIter.hasNext() && actIter.hasNext()) {
					final Tuple2<Integer, Row> expNext = expIter.next();
					final Tuple2<Integer, Row> actNext = actIter.next();
					if (!expNext.equals(actNext)) {
						out.collect("Expected: " + expNext + " | Actual: " + actNext);
					}
				}
				while (expIter.hasNext()) {
					final Tuple2<Integer, Row> expNext = expIter.next();
					out.collect("Missing: " + expNext);
				}
				while (actIter.hasNext()) {
					final Tuple2<Integer, Row> actNext = actIter.next();
					out.collect("Not expected: " + actNext);
				}
			}
		});

		result.writeAsText(validatePath + "/simple");
	}

	private static void validateMany(BatchTableEnvironment tenv, String inPath, String outPath, String validatePath) {

	}

	public static class NumberingMapPartionFunction<T> implements MapPartitionFunction<T, Tuple2<Integer, T>> {

		private int idx = 0;

		@Override
		public void mapPartition(Iterable<T> iterable, Collector<Tuple2<Integer, T>> out) throws Exception {
			for (T value : iterable) {
				out.collect(new Tuple2<>(idx++, value));
			}
		}
	}
}
