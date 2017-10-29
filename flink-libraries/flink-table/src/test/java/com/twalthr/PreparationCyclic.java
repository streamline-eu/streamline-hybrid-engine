package com.twalthr;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class PreparationCyclic {

	public static void main(String[] args) throws Exception {
		final String inPath;
		final int seconds;
		final String outPath;
		if (args.length == 0) {
			inPath = "/Users/twalthr/flink/data/mt/Stackoverflow/in/split";
			seconds = 2;
			outPath = "/Users/twalthr/flink/data/mt/Stackoverflow/prepared";
		} else {
			inPath = args[0];
			seconds = Integer.parseInt(args[1]);
			outPath = args[2];
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		BatchTableEnvironment tenv = new BatchTableEnvironment(env, TableConfig.DEFAULT());

		{
			final CsvTableSource stackoverflow = CsvTableSource.builder()
					.path(inPath + "/sx-stackoverflow.txt")
					.fieldDelimiter(" ")
					.field("src", Types.INT())
					.field("dst", Types.INT())
					.field("ts", Types.LONG())
					.build();

			tenv.registerTableSource("stackoverflow", stackoverflow);
		}

		Table ts = tenv.sql(
				"SELECT ts, CASE WHEN src > dst THEN dst ELSE src END, CASE WHEN src > dst THEN src ELSE dst END " +
				"FROM stackoverflow " +
				"ORDER BY ts");

		DataSet<Row> stackoverflowDataSet = tenv.toDataSet(ts, Row.class);

		Table stackoverflowTable = tenv.fromDataSet(
				stackoverflowDataSet.mapPartition(new OutOfOrderFunction<>(seconds)),
				"ts, src, dst");

		stackoverflowTable.writeToSink(new CsvTableSink(outPath + "/stackoverflow", "|"));

		env.execute();
	}

	public static class OutOfOrderFunction<T> extends RichMapPartitionFunction<T, T> {

		private int seconds;

		List<T> values;

		public OutOfOrderFunction(int seconds) {
			this.seconds = seconds;
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
				} else if (last_ts < next_ts - TimeUnit.SECONDS.toMillis(seconds)) {
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
