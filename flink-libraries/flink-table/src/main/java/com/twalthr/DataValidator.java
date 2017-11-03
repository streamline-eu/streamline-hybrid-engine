package com.twalthr;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;

public class DataValidator extends AbstractStreamOperator<Row> implements OneInputStreamOperator<Tuple2<Boolean, Row>, Row> {

	private long insertCount = 0;
	private long deleteCount = 0;

	private transient HashMap<Long, Long> throughput = null;
	private transient HashMap<Long, Double> latency = null;

	private volatile long countSinceLast = 0;
	private volatile long latencySumSinceLast = 0;

	@Override
	public void open() throws Exception {
		this.throughput = new HashMap<>();
		this.latency = new HashMap<>();


		getProcessingTimeService().scheduleAtFixedRate(new ProcessingTimeCallback() {
			@Override
			public void onProcessingTime(long l) throws Exception {
				if (countSinceLast > 0) {
					synchronized (this) {
						throughput.put(l, countSinceLast);
						latency.put(l, ((double) latencySumSinceLast) / countSinceLast);
						countSinceLast = 0L;
						latencySumSinceLast = 0L;
					}
				}
			}
		}, 1000L, 1000L);
	}

	@Override
	public void processElement(StreamRecord<Tuple2<Boolean, Row>> element) throws Exception {
		Tuple2<Boolean, Row> record = element.getValue();
		if (record.f0) {
			insertCount++;
		} else {
			deleteCount++;
		}
		countSinceLast++;
		latencySumSinceLast += System.currentTimeMillis() - element.getTimestamp();
	}

	@Override
	public void close() throws Exception {
		Row out = new Row(2);
		out.setField(0, insertCount);
		out.setField(1, deleteCount);
		final StringBuilder sb = new StringBuilder();
		sb.append("BASIC RESULT: ").append(out);
		synchronized (this) {
			sb.append("\n\nTHROUGHPUT:");
			for (Map.Entry<Long, Long> entry : throughput.entrySet()) {
				sb.append("\n").append(entry.getKey()).append("|").append(entry.getValue());
			}
			sb.append("\n\nLATENCY:");
			for (Map.Entry<Long, Double> entry : latency.entrySet()) {
				sb.append("\n").append(entry.getKey()).append("|").append(entry.getValue());
			}
		}
		System.out.println(sb.toString());
	}
}
