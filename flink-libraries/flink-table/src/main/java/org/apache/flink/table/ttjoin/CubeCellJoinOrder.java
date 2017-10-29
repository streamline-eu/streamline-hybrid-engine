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

package org.apache.flink.table.ttjoin;

/**
 * Calculates an optimal order for HybridHyperCube.
 */
public class CubeCellJoinOrder {

	private final double[] cardinalities; // per table, map to cardinalities
	private final double[][] singleKeyDistinctCountMap; // per table and single key, map to distinct count
	private final int[][][] keySetTablesSingleKeysMap; // per key set, map to input and single key

	private int[] bestTotalOrder;
	private double bestCost;

	public CubeCellJoinOrder(
			final double[] cardinalities,
			final double[][] singleKeyDistinctCountMap,
			final int[][][] keySetTablesSingleKeysMap) {
		this.cardinalities = cardinalities;
		this.singleKeyDistinctCountMap = singleKeyDistinctCountMap;
		this.keySetTablesSingleKeysMap = keySetTablesSingleKeysMap;

		calculateTotalOrder();
	}

	private void calculateTotalOrder() {
		bestCost = Double.MAX_VALUE;
		bestTotalOrder = new int[keySetTablesSingleKeysMap.length];

		final int[] keySetsCombination = new int[keySetTablesSingleKeysMap.length];
		// set key sets
		for (int i = 0; i < keySetsCombination.length; ++i) {
			keySetsCombination[i] = i;
		}
		evaluatePermutations(keySetsCombination, 0);
	}

	private void evaluatePermutations(final int[] combination, final int start) {
		// source: https://stackoverflow.com/a/36139335/806430

		if (start == combination.length) {
			evaluateCost(combination);
			return;
		}

		for (int i = start; i < combination.length; ++i) {
			// swapping
			final int temp = combination[i];
			combination[i] = combination[start];
			combination[start] = temp;

			evaluatePermutations(combination, start + 1);

			// back swapping
			final int temp2 = combination[i];
			combination[i] = combination[start];
			combination[start] = temp2;
		}
	}

	private void evaluateCost(final int[] combination) {
		final double cost = computeCost(combination);
		if (cost < bestCost) {
			bestCost = cost;
			System.arraycopy(combination, 0, bestTotalOrder, 0, combination.length);
		}
	}

	private double computeCost(final int[] combination) {
		double cost = 0;
		for (int i = 0; i < combination.length; ++i) {
			cost += computeStep(combination, i) + computeStep(combination, i) * computeStep(combination, i + 1);
		}
		return cost;
	}

	private double computeStep(final int[] combination, final int step) {
		if (step == 0) {
			double cost = Double.MAX_VALUE;
			final int[][] tablesAndSingleKeys = keySetTablesSingleKeysMap[combination[step]];
			// find min of current single key of all tables
			for (final int[] tableAndSingleKey : tablesAndSingleKeys) {
				final int table = tableAndSingleKey[0];
				final int singleKey = tableAndSingleKey[1];
				cost = Math.min(cost, singleKeyDistinctCountMap[table][singleKey]);
			}
			return cost;
		} else if (step < combination.length) {
			double cost = Double.MAX_VALUE;
			// determine all tables that are involved in current step
			final int[][] tablesAndSingleKeys = keySetTablesSingleKeysMap[combination[step]];
			for (final int[] tableAndSingleKey : tablesAndSingleKeys) {
				final int table = tableAndSingleKey[0];

				// estimate distinct count for each key set of the combination until current step (inclusive)
				double distinctCountCur = calculateDistinctCount(combination, table, step);

				// estimate distinct count for each key set of the combination until current step (exclusive)
				double distinctCountPrev = calculateDistinctCount(combination, table, step - 1);

				// estimate costs for intersection
				cost = Math.min(cost, distinctCountCur / distinctCountPrev);
			}
			return cost;
		}
		return 0;
	}

	private double calculateDistinctCount(final int[] combination, final int table, final int maxKeySetPos) {
 		double distinctCount = 1;
		for (int keySetPos = 0; keySetPos <= maxKeySetPos; ++keySetPos) {
			final int keySet = combination[keySetPos];
			// get all involved tables for this key set
			final int[][] tablesAndSingleKeys = keySetTablesSingleKeysMap[keySet];
			for (final int[] curTableAndSingleKey : tablesAndSingleKeys) {
				final int curTable = curTableAndSingleKey[0];
				final int curSingleKey = curTableAndSingleKey[1];
				// check if involved table is table of current step
				// thus key set is used by this table
				if (curTable == table) {
					// multiple distinct count of every single key
					distinctCount *= singleKeyDistinctCountMap[table][curSingleKey];
				}
			}
		}

		if (maxKeySetPos == 0) {
			return distinctCount;
		}
		return Math.min(cardinalities[table] / 2, distinctCount);
	}

	public int[] getBestTotalOrder() {
		return bestTotalOrder;
	}
}
