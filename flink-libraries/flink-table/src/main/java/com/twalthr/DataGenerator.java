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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;

import com.google.common.base.Splitter;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

public class DataGenerator extends RichParallelSourceFunction<Row> {

	private final Parser parser;
	private final String path;
	private final long servingSpeed;

	private transient int para;
	private transient int index;
	private transient FileInputStream fis;
	private transient BufferedReader reader;

	// each partition must be ordered according to timestamp
	// it might be slightly out of order
	public DataGenerator(String table, String path, long servingSpeed) {
		this.path = path;
		this.servingSpeed = servingSpeed;
		this.parser = getParser(table);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		this.para = getRuntimeContext().getNumberOfParallelSubtasks();
		this.index = getRuntimeContext().getIndexOfThisSubtask();
	}

	@Override
	public void run(SourceContext<Row> ctx) throws Exception {
		fis = new FileInputStream(path + "/para" + para + "/" + index);
		reader = new BufferedReader(new InputStreamReader(fis), 1024 * 1024);
		final Parser p = parser;
		final BufferedReader r = reader;

		// read first record
		String line;
		Row record;
		while (r.ready() && (line = r.readLine()) != null) {
			record = p.parse(line);

			if (servingSpeed > 0) {
				Thread.sleep(servingSpeed);
			}

			ctx.collect(record);
		}
	}

	@Override
	public void cancel() {
		try {
			if (this.reader != null) {
				this.reader.close();
			}
			if (this.fis != null) {
				this.fis.close();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	// --------------------------------------------------------------------------------------------

	public Parser getParser(String table) {
		switch (table) {

			case "customer":
				return new Parser() {
					private final Row row = new Row(9);
					private final Splitter splitter = Splitter.on('|');
					@Override
					public Row parse(String line) {
						final Iterator<String> split = splitter.split(line).iterator();
						row.setField(0, Long.parseLong(split.next()));     // c_ts
						row.setField(1, Integer.parseInt(split.next()));   // c_custkey
						row.setField(2, split.next());                     // c_name
						row.setField(3, split.next());                     // c_address
						row.setField(4, Integer.parseInt(split.next()));   // c_nationkey
						row.setField(5, split.next());                     // c_phone
						row.setField(6, Double.parseDouble(split.next())); // c_acctbal
						row.setField(7, split.next());                     // c_mktsegment
						row.setField(8, split.next());                     // c_comment
						return row;
					}
				};

			case "order":
				return new Parser() {
					private final Row row = new Row(10);
					private final Splitter splitter = Splitter.on('|');
					@Override
					public Row parse(String line) {
						final Iterator<String> split = splitter.split(line).iterator();
						row.setField(0, Long.parseLong(split.next()));     // o_ts
						row.setField(1, Integer.parseInt(split.next()));   // o_orderkey
						row.setField(2, Integer.parseInt(split.next()));   // o_custkey
						row.setField(3, split.next());                     // o_orderstatus
						row.setField(4, Double.parseDouble(split.next())); // o_totalprice
						row.setField(5, split.next());                     // o_orderdate
						row.setField(6, split.next());                     // o_orderpriority
						row.setField(7, split.next());                     // o_clerk
						row.setField(8, Integer.parseInt(split.next()));   // o_shippriority
						row.setField(9, split.next());                     // o_comment
						return row;
					}
				};

			case "lineitem":
				return new Parser() {
					private final Row row = new Row(17);
					private final Splitter splitter = Splitter.on('|');
					@Override
					public Row parse(String line) {
						final Iterator<String> split = splitter.split(line).iterator();
						row.setField(0, Long.parseLong(split.next()));     // l_ts
						row.setField(1, Integer.parseInt(split.next()));   // l_orderkey
						row.setField(2, Integer.parseInt(split.next()));   // l_partkey
						row.setField(3, Integer.parseInt(split.next()));   // l_suppkey
						row.setField(4, Integer.parseInt(split.next()));   // l_linenumber
						row.setField(5, Integer.parseInt(split.next()));   // l_quantity
						row.setField(6, Double.parseDouble(split.next())); // l_extendedprice
						row.setField(7, Double.parseDouble(split.next())); // l_discount
						row.setField(8, Double.parseDouble(split.next())); // l_tax
						row.setField(9, split.next());                     // l_returnflag
						row.setField(10, split.next());                    // l_linestatus
						row.setField(11, split.next());                    // l_shipdate
						row.setField(12, split.next());                    // l_commitdate
						row.setField(13, split.next());                    // l_receiptdate
						row.setField(14, split.next());                    // l_shipinstruct
						row.setField(15, split.next());                    // l_shipmode
						row.setField(16, split.next());                    // l_comment
						return row;
					}
				};

			case "supplier":
				return new Parser() {
					private final Row row = new Row(8);
					private final Splitter splitter = Splitter.on('|');
					@Override
					public Row parse(String line) {
						final Iterator<String> split = splitter.split(line).iterator();
						row.setField(0, Long.parseLong(split.next()));     // s_ts
						row.setField(1, Integer.parseInt(split.next()));   // s_suppkey
						row.setField(2, split.next());                     // s_name
						row.setField(3, split.next());                     // s_address
						row.setField(4, Integer.parseInt(split.next()));   // s_nationkey
						row.setField(5, split.next());                     // s_phone
						row.setField(6, Double.parseDouble(split.next())); // s_acctbal
						row.setField(7, split.next());                     // s_comment
						return row;
					}
				};

			case "nation":
				return new Parser() {
					private final Row row = new Row(5);
					private final Splitter splitter = Splitter.on('|');
					@Override
					public Row parse(String line) {
						final Iterator<String> split = splitter.split(line).iterator();
						row.setField(0, Long.parseLong(split.next()));   // n_ts
						row.setField(1, Integer.parseInt(split.next())); // n_nationkey
						row.setField(2, split.next());                   // n_name
						row.setField(3, Integer.parseInt(split.next())); // n_regionkey
						row.setField(4, split.next());                   // n_comment
						return row;
					}
				};

			case "region":
				return new Parser() {
					private final Row row = new Row(4);
					private final Splitter splitter = Splitter.on('|');
					@Override
					public Row parse(String line) {
						final Iterator<String> split = splitter.split(line).iterator();
						row.setField(0, Long.parseLong(split.next()));   // r_ts
						row.setField(1, Integer.parseInt(split.next())); // r_regionkey
						row.setField(2, split.next());                   // r_name
						row.setField(3, split.next());                   // r_comment
						return null;
					}
				};

			case "edges":
				return new Parser() {
					private final Row row = new Row(3);
					private final Splitter splitter = Splitter.on('|');
					@Override
					public Row parse(String line) {
						final Iterator<String> split = splitter.split(line).iterator();
						row.setField(0, Long.parseLong(split.next()));   // ts
						row.setField(1, Integer.parseInt(split.next())); // src
						row.setField(2, Integer.parseInt(split.next())); // dst
						return row;
					}
				};
		}
		throw new IllegalArgumentException();
	}

	private interface Parser {
		Row parse(String line);
	}
}
