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

import org.junit.Test;

public class QueriesTests {

	// --------------------------------------------------------------------------------------------
	// SIMPLE HEAP
	// --------------------------------------------------------------------------------------------

	@Test
	public void testSimpleStreamHeapPara1() throws Exception {
		Queries.run("simple", 1, 1, false, true, 0, 0L, true, true, true, "",
			"/Users/twalthr/flink/data/mt/S0001/prepared", "/Users/twalthr/flink/data/mt/S0001/result",
			10, 0.0001, -1, -1, -1, false, false);
		ValidationSimpleAndMany.run("simple",
			"/Users/twalthr/flink/data/mt/S0001/prepared",
			"/Users/twalthr/flink/data/mt/S0001/result",
			"/Users/twalthr/flink/data/mt/S0001/validate");
	}

	@Test
	public void testSimpleStreamHeapPara8() throws Exception {
		Queries.run("simple", 8, 8, false, true, 0, 0L, true, true, true, "",
			"/Users/twalthr/flink/data/mt/S0001/prepared", "/Users/twalthr/flink/data/mt/S0001/result",
			10, 0.0001, -1, -1, -1, false, false);
		ValidationSimpleAndMany.run("simple",
			"/Users/twalthr/flink/data/mt/S0001/prepared",
			"/Users/twalthr/flink/data/mt/S0001/result",
			"/Users/twalthr/flink/data/mt/S0001/validate");
	}

	@Test
	public void testSimpleStreamHeapPara1WithMax16() throws Exception {
		Queries.run("simple", 16, 1, false, true, 0, 0L, true, true, true, "",
			"/Users/twalthr/flink/data/mt/S0001/prepared", "/Users/twalthr/flink/data/mt/S0001/result",
			10, 0.0001, -1, -1, -1, false, false);
		ValidationSimpleAndMany.run("simple",
			"/Users/twalthr/flink/data/mt/S0001/prepared",
			"/Users/twalthr/flink/data/mt/S0001/result",
			"/Users/twalthr/flink/data/mt/S0001/validate");
	}

	@Test
	public void testSimpleStreamHeapPara8WithMax16() throws Exception {
		Queries.run("simple", 16, 8, false, true, 0, 0L, true, true, true, "",
			"/Users/twalthr/flink/data/mt/S0001/prepared", "/Users/twalthr/flink/data/mt/S0001/result",
			10, 0.0001, -1, -1, -1, false, false);
		ValidationSimpleAndMany.run("simple",
			"/Users/twalthr/flink/data/mt/S0001/prepared",
			"/Users/twalthr/flink/data/mt/S0001/result",
			"/Users/twalthr/flink/data/mt/S0001/validate");
	}

	@Test
	public void testSimpleWatermarkHeapPara1() throws Exception {
		Queries.run("simple", 1, 1, true, true, 1, 0L, true, true, true, "",
			"/Users/twalthr/flink/data/mt/S0001/prepared", "/Users/twalthr/flink/data/mt/S0001/result",
			10, 0.0001, -1, -1, -1, false, false);
		ValidationSimpleAndMany.run("simple",
			"/Users/twalthr/flink/data/mt/S0001/prepared",
			"/Users/twalthr/flink/data/mt/S0001/result",
			"/Users/twalthr/flink/data/mt/S0001/validate");
	}

	@Test
	public void testSimpleWatermarkHeapPara8() throws Exception {
		Queries.run("simple", 8, 8, true, true, 1, 0L, true, true, true, "",
			"/Users/twalthr/flink/data/mt/S0001/prepared", "/Users/twalthr/flink/data/mt/S0001/result",
			10, 0.0001, -1, -1, -1, false, false);
		ValidationSimpleAndMany.run("simple",
			"/Users/twalthr/flink/data/mt/S0001/prepared",
			"/Users/twalthr/flink/data/mt/S0001/result",
			"/Users/twalthr/flink/data/mt/S0001/validate");
	}

	@Test
	public void testSimpleWatermarkHeapPara1WithMax16() throws Exception {
		Queries.run("simple", 16, 1, true, true, 1, 0L, true, true, true, "",
			"/Users/twalthr/flink/data/mt/S0001/prepared", "/Users/twalthr/flink/data/mt/S0001/result",
			10, 0.0001, -1, -1, -1, false, false);
		ValidationSimpleAndMany.run("simple",
			"/Users/twalthr/flink/data/mt/S0001/prepared",
			"/Users/twalthr/flink/data/mt/S0001/result",
			"/Users/twalthr/flink/data/mt/S0001/validate");
	}

	@Test
	public void testSimpleWatermarkHeapPara8WithMax16() throws Exception {
		Queries.run("simple", 16, 8, true, true, 1, 0L, true, true, true, "",
			"/Users/twalthr/flink/data/mt/S0001/prepared", "/Users/twalthr/flink/data/mt/S0001/result",
			10, 0.0001, -1, -1, -1, false, false);
		ValidationSimpleAndMany.run("simple",
			"/Users/twalthr/flink/data/mt/S0001/prepared",
			"/Users/twalthr/flink/data/mt/S0001/result",
			"/Users/twalthr/flink/data/mt/S0001/validate");
	}

	@Test
	public void testSimplePeriodicProctimeHeapPara1() throws Exception {
		Queries.run("simple", 1, 1, false, true, 2, 100L, true, true, true, "",
			"/Users/twalthr/flink/data/mt/S0001/prepared", "/Users/twalthr/flink/data/mt/S0001/result",
			10, 0.0001, -1, -1, -1, false, false);
		ValidationSimpleAndMany.run("simple",
			"/Users/twalthr/flink/data/mt/S0001/prepared",
			"/Users/twalthr/flink/data/mt/S0001/result",
			"/Users/twalthr/flink/data/mt/S0001/validate");
	}

	@Test
	public void testSimplePeriodicProctimeHeapPara8WithMax16() throws Exception {
		Queries.run("simple", 16, 8, false, true, 2, 100L, true, true, true, "",
			"/Users/twalthr/flink/data/mt/S0001/prepared", "/Users/twalthr/flink/data/mt/S0001/result",
			10, 0.0001, -1, -1, -1, false, false);
		ValidationSimpleAndMany.run("simple",
			"/Users/twalthr/flink/data/mt/S0001/prepared",
			"/Users/twalthr/flink/data/mt/S0001/result",
			"/Users/twalthr/flink/data/mt/S0001/validate");
	}

	@Test
	public void testSimplePeriodicRowtimeHeapPara1() throws Exception {
		Queries.run("simple", 1, 1, true, true, 2, 100L, true, true, true, "",
			"/Users/twalthr/flink/data/mt/S0001/prepared", "/Users/twalthr/flink/data/mt/S0001/result",
			10, 0.0001, -1, -1, -1, false, false);
		ValidationSimpleAndMany.run("simple",
			"/Users/twalthr/flink/data/mt/S0001/prepared",
			"/Users/twalthr/flink/data/mt/S0001/result",
			"/Users/twalthr/flink/data/mt/S0001/validate");
	}

	@Test
	public void testSimplePeriodicRowtimeHeapPara8WithMax16() throws Exception {
		Queries.run("simple", 16, 8, true, true, 2, 100L, true, true, true, "",
			"/Users/twalthr/flink/data/mt/S0001/prepared", "/Users/twalthr/flink/data/mt/S0001/result",
			10, 0.0001, -1, -1, -1, false, false);
		ValidationSimpleAndMany.run("simple",
			"/Users/twalthr/flink/data/mt/S0001/prepared",
			"/Users/twalthr/flink/data/mt/S0001/result",
			"/Users/twalthr/flink/data/mt/S0001/validate");
	}

	// --------------------------------------------------------------------------------------------
	// SIMPLE ROCKSDB
	// --------------------------------------------------------------------------------------------

	@Test
	public void testSimpleStreamRocksPara1() throws Exception {
		Queries.run("simple", 1, 1, false, true, 0, 0L, false, true, true, "file:///Users/twalthr/flink/tmp/checkpoints",
			"/Users/twalthr/flink/data/mt/S0001/prepared", "/Users/twalthr/flink/data/mt/S0001/result",
			10, 0.0001, -1, -1, -1, false, false);
		ValidationSimpleAndMany.run("simple",
			"/Users/twalthr/flink/data/mt/S0001/prepared",
			"/Users/twalthr/flink/data/mt/S0001/result",
			"/Users/twalthr/flink/data/mt/S0001/validate");
	}

	@Test
	public void testSimpleStreamRocksPara8() throws Exception {
		Queries.run("simple", 8, 8, false, true, 0, 0L, false, true, true, "file:///Users/twalthr/flink/tmp/checkpoints",
			"/Users/twalthr/flink/data/mt/S0001/prepared", "/Users/twalthr/flink/data/mt/S0001/result",
			10, 0.0001, -1, -1, -1, false, false);
		ValidationSimpleAndMany.run("simple",
			"/Users/twalthr/flink/data/mt/S0001/prepared",
			"/Users/twalthr/flink/data/mt/S0001/result",
			"/Users/twalthr/flink/data/mt/S0001/validate");
	}

	@Test
	public void testSimpleStreamRocksPara1WithMax16() throws Exception {
		Queries.run("simple", 16, 1, false, true, 0, 0L, false, true, true, "file:///Users/twalthr/flink/tmp/checkpoints",
			"/Users/twalthr/flink/data/mt/S0001/prepared", "/Users/twalthr/flink/data/mt/S0001/result",
			10, 0.0001, -1, -1, -1, false, false);
		ValidationSimpleAndMany.run("simple",
			"/Users/twalthr/flink/data/mt/S0001/prepared",
			"/Users/twalthr/flink/data/mt/S0001/result",
			"/Users/twalthr/flink/data/mt/S0001/validate");
	}

	@Test
	public void testSimpleStreamRocksPara8WithMax16() throws Exception {
		Queries.run("simple", 16, 8, false, true, 0, 0L, false, true, true, "file:///Users/twalthr/flink/tmp/checkpoints",
			"/Users/twalthr/flink/data/mt/S0001/prepared", "/Users/twalthr/flink/data/mt/S0001/result",
			10, 0.0001, -1, -1, -1, false, false);
		ValidationSimpleAndMany.run("simple",
			"/Users/twalthr/flink/data/mt/S0001/prepared",
			"/Users/twalthr/flink/data/mt/S0001/result",
			"/Users/twalthr/flink/data/mt/S0001/validate");
	}

	@Test
	public void testSimpleWatermarkRocksPara1() throws Exception {
		Queries.run("simple", 1, 1, true, true, 1, 0L, false, true, true, "file:///Users/twalthr/flink/tmp/checkpoints",
			"/Users/twalthr/flink/data/mt/S0001/prepared", "/Users/twalthr/flink/data/mt/S0001/result",
			10, 0.0001, -1, -1, -1, false, false);
		ValidationSimpleAndMany.run("simple",
			"/Users/twalthr/flink/data/mt/S0001/prepared",
			"/Users/twalthr/flink/data/mt/S0001/result",
			"/Users/twalthr/flink/data/mt/S0001/validate");
	}

	@Test
	public void testSimpleWatermarkRocksPara8() throws Exception {
		Queries.run("simple", 8, 8, true, true, 1, 0L, false, true, true, "file:///Users/twalthr/flink/tmp/checkpoints",
			"/Users/twalthr/flink/data/mt/S0001/prepared", "/Users/twalthr/flink/data/mt/S0001/result",
			10, 0.0001, -1, -1, -1, false, false);
		ValidationSimpleAndMany.run("simple",
			"/Users/twalthr/flink/data/mt/S0001/prepared",
			"/Users/twalthr/flink/data/mt/S0001/result",
			"/Users/twalthr/flink/data/mt/S0001/validate");
	}

	@Test
	public void testSimpleWatermarkRocksPara1WithMax16() throws Exception {
		Queries.run("simple", 16, 1, true, true, 1, 0L, false, true, true, "file:///Users/twalthr/flink/tmp/checkpoints",
			"/Users/twalthr/flink/data/mt/S0001/prepared", "/Users/twalthr/flink/data/mt/S0001/result",
			10, 0.0001, -1, -1, -1, false, false);
		ValidationSimpleAndMany.run("simple",
			"/Users/twalthr/flink/data/mt/S0001/prepared",
			"/Users/twalthr/flink/data/mt/S0001/result",
			"/Users/twalthr/flink/data/mt/S0001/validate");
	}

	@Test
	public void testSimpleWatermarkRocksPara8WithMax16() throws Exception {
		Queries.run("simple", 16, 8, true, true, 1, 0L, false, true, true, "file:///Users/twalthr/flink/tmp/checkpoints",
			"/Users/twalthr/flink/data/mt/S0001/prepared", "/Users/twalthr/flink/data/mt/S0001/result",
			10, 0.0001, -1, -1, -1, false, false);
		ValidationSimpleAndMany.run("simple",
			"/Users/twalthr/flink/data/mt/S0001/prepared",
			"/Users/twalthr/flink/data/mt/S0001/result",
			"/Users/twalthr/flink/data/mt/S0001/validate");
	}

	@Test
	public void testSimplePeriodicProctimeRocksPara1() throws Exception {
		Queries.run("simple", 1, 1, false, true, 2, 100L, false, true, true, "file:///Users/twalthr/flink/tmp/checkpoints",
			"/Users/twalthr/flink/data/mt/S0001/prepared", "/Users/twalthr/flink/data/mt/S0001/result",
			10, 0.0001, -1, -1, -1, false, false);
		ValidationSimpleAndMany.run("simple",
			"/Users/twalthr/flink/data/mt/S0001/prepared",
			"/Users/twalthr/flink/data/mt/S0001/result",
			"/Users/twalthr/flink/data/mt/S0001/validate");
	}

	@Test
	public void testSimplePeriodicProctimeRocksPara8WithMax16() throws Exception {
		Queries.run("simple", 16, 8, false, true, 2, 100L, false, true, true, "file:///Users/twalthr/flink/tmp/checkpoints",
			"/Users/twalthr/flink/data/mt/S0001/prepared", "/Users/twalthr/flink/data/mt/S0001/result",
			10, 0.0001, -1, -1, -1, false, false);
		ValidationSimpleAndMany.run("simple",
			"/Users/twalthr/flink/data/mt/S0001/prepared",
			"/Users/twalthr/flink/data/mt/S0001/result",
			"/Users/twalthr/flink/data/mt/S0001/validate");
	}

	@Test
	public void testSimplePeriodicRowtimeRocksPara1() throws Exception {
		Queries.run("simple", 1, 1, true, true, 2, 100L, false, true, true, "file:///Users/twalthr/flink/tmp/checkpoints",
			"/Users/twalthr/flink/data/mt/S0001/prepared", "/Users/twalthr/flink/data/mt/S0001/result",
			10, 0.0001, -1, -1, -1, false, false);
		ValidationSimpleAndMany.run("simple",
			"/Users/twalthr/flink/data/mt/S0001/prepared",
			"/Users/twalthr/flink/data/mt/S0001/result",
			"/Users/twalthr/flink/data/mt/S0001/validate");
	}

	@Test
	public void testSimplePeriodicRowtimeRocksPara8WithMax16() throws Exception {
		Queries.run("simple", 16, 8, true, true, 2, 100L, false, true, true, "file:///Users/twalthr/flink/tmp/checkpoints",
			"/Users/twalthr/flink/data/mt/S0001/prepared", "/Users/twalthr/flink/data/mt/S0001/result",
			10, 0.0001, -1, -1, -1, false, false);
		ValidationSimpleAndMany.run("simple",
			"/Users/twalthr/flink/data/mt/S0001/prepared",
			"/Users/twalthr/flink/data/mt/S0001/result",
			"/Users/twalthr/flink/data/mt/S0001/validate");
	}

	// --------------------------------------------------------------------------------------------
	// MANY HEAP
	// --------------------------------------------------------------------------------------------

	@Test
	public void testManyNoAggStreamHeapPara1() throws Exception {
		Queries.run("many", 1, 1, false, true, 0, 0L, true, true, true, "",
			"/Users/twalthr/flink/data/mt/S01/prepared", "/Users/twalthr/flink/data/mt/S01/result",
			10, 0.01, -1, -1, -1, false, true);
		ValidationSimpleAndMany.run("many",
			"/Users/twalthr/flink/data/mt/S01/prepared",
			"/Users/twalthr/flink/data/mt/S01/result",
			"/Users/twalthr/flink/data/mt/S01/validate");
	}

	@Test
	public void testManyNoAggStreamHeapPara8() throws Exception {
		Queries.run("many", 8, 8, false, true, 0, 0L, true, true, true, "",
			"/Users/twalthr/flink/data/mt/S01/prepared", "/Users/twalthr/flink/data/mt/S01/result",
			10, 0.01, -1, -1, -1, false, true);
		ValidationSimpleAndMany.run("many",
			"/Users/twalthr/flink/data/mt/S01/prepared",
			"/Users/twalthr/flink/data/mt/S01/result",
			"/Users/twalthr/flink/data/mt/S01/validate");
	}

	@Test
	public void testManyNoAggStreamHeapPara1WithMax16() throws Exception {
		Queries.run("many", 16, 1, false, true, 0, 0L, true, true, true, "",
			"/Users/twalthr/flink/data/mt/S01/prepared", "/Users/twalthr/flink/data/mt/S01/result",
			10, 0.01, -1, -1, -1, false, true);
		ValidationSimpleAndMany.run("many",
			"/Users/twalthr/flink/data/mt/S01/prepared",
			"/Users/twalthr/flink/data/mt/S01/result",
			"/Users/twalthr/flink/data/mt/S01/validate");
	}

	@Test
	public void testManyNoAggStreamHeapPara8WithMax16() throws Exception {
		Queries.run("many", 16, 8, false, true, 0, 0L, true, true, true, "",
			"/Users/twalthr/flink/data/mt/S01/prepared", "/Users/twalthr/flink/data/mt/S01/result",
			10, 0.01, -1, -1, -1, false, true);
		ValidationSimpleAndMany.run("many",
			"/Users/twalthr/flink/data/mt/S01/prepared",
			"/Users/twalthr/flink/data/mt/S01/result",
			"/Users/twalthr/flink/data/mt/S01/validate");
	}

	@Test
	public void testManyNoAggWatermarkHeapPara1() throws Exception {
		Queries.run("many", 1, 1, true, true, 1, 0L, true, true, true, "",
			"/Users/twalthr/flink/data/mt/S01/prepared", "/Users/twalthr/flink/data/mt/S01/result",
			10, 0.01, -1, -1, -1, false, true);
//		ValidationSimpleAndMany.run("many",
//			"/Users/twalthr/flink/data/mt/S01/prepared",
//			"/Users/twalthr/flink/data/mt/S01/result",
//			"/Users/twalthr/flink/data/mt/S01/validate");
	}

	@Test
	public void testManyNoAggWatermarkHeapPara8() throws Exception {
		Queries.run("many", 8, 8, false, true, 1, 0L, true, true, true, "",
			"/Users/twalthr/flink/data/mt/S01/prepared", "/Users/twalthr/flink/data/mt/S01/result",
			10, 0.01, -1, -1, -1, false, true);
		ValidationSimpleAndMany.run("many",
			"/Users/twalthr/flink/data/mt/S01/prepared",
			"/Users/twalthr/flink/data/mt/S01/result",
			"/Users/twalthr/flink/data/mt/S01/validate");
	}
}
