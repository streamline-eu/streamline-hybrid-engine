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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Row;

public class DataValidator extends AbstractStreamOperator<Row> implements OneInputStreamOperator<Tuple2<Boolean, Row>, Row> {

	private int insertCount = 0;
	private int deleteCount = 0;

	@Override
	public void processElement(StreamRecord<Tuple2<Boolean, Row>> element) throws Exception {
		Tuple2<Boolean, Row> record = element.getValue();
		if (record.f0) {
			insertCount++;
		} else {
			deleteCount++;
		}
	}

	@Override
	public void close() throws Exception {
		Row out = new Row(2);
		out.setField(0, insertCount);
		out.setField(1, deleteCount);
		output.collect(new StreamRecord<>(out));
	}
}
