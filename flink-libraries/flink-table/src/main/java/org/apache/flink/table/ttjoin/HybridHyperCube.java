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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Preconditions;

import com.google.common.primitives.Ints;
import org.davidmoten.hilbert.HilbertCurve;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Calculates a HybridHyperCube partitioning.
 */
public final class HybridHyperCube {

	// input
	private final int totalKeyOrderArity;
	private final int maxParallelism;
	private final double[] cardinalities;
	private final int[][] tableKeyTotalOrderKeysMap;
	private final boolean[][] tableKeySkewMap;

	// results and intermediate results
	private int dimensions;
	private double bestWorkload;
	private int[] bestConfiguration;
	private int bestMaxFactor;
	private int bestMaxParallelism;
	// per table: key to dimension (starts at 1)
	// 0 = replication, negative dimension = random partitioning; positive dimension = hash partitioning
	private int[][] tableKeyDimensionMap;
	private int[][] tableDimensionsMap; // all involved dimensions for a table R
	private Map<CubePosition, Integer> hyperCubeMap; // position to cell
	private List<Map<CubePosition, CubeGroup>> tablePosGroupMap; // per table: map a position (a, b, *) to hyper cube cell group
	private int[][] groupCellsMap; // map a cell group id to hyper cube cells

	public HybridHyperCube(
			final int totalKeyOrderArity,
			final int maxParallelism,
			final double[] cardinalities,
			final int[][] tableKeyTotalOrderKeysMap,
			final boolean[][] tableKeySkewMap) {

		Preconditions.checkArgument(cardinalities.length == tableKeyTotalOrderKeysMap.length);
		Preconditions.checkArgument(cardinalities.length == tableKeySkewMap.length);

		this.cardinalities = cardinalities;
		this.totalKeyOrderArity = totalKeyOrderArity;
		this.maxParallelism = maxParallelism;
		this.tableKeyTotalOrderKeysMap = tableKeyTotalOrderKeysMap;
		this.tableKeySkewMap = tableKeySkewMap;

		this.bestWorkload = Double.POSITIVE_INFINITY;

		calculateDimensions();

		calculateDimensionSizes();

		calculateHyperCubeMap();

		calculatePosCellsMap();
	}

	private void calculateDimensions() {
		// dimensionality adaption
		final Map<Integer, List<Tuple2<Integer, Integer>>> tableKeyDimensionMap = new HashMap<>();
		final Map<Integer, List<Integer>> tableDimensionsMap = new HashMap<>();
		int currentKey = 0;

		// add all dimensions
		// R(x, y) x S(y, z) x T(z, x) -> [x, y, z]
		// for each key check for non-skewed usage for new dimensions
		for (int totalOrderKey = 0; totalOrderKey < totalKeyOrderArity; ++totalOrderKey) {
			final List<Tuple2<Integer, Integer>> tablesWithKeys = new ArrayList<>();
			// for each table
			for (int tableIdx = 0; tableIdx < tableKeyTotalOrderKeysMap.length; ++tableIdx) {
				final int[] keyTotalOrderKeysMap = tableKeyTotalOrderKeysMap[tableIdx];
				// for each key of the table
				for (int key = 0; key < keyTotalOrderKeysMap.length; ++key) {
					// table contains total order key and is not skewed
					if (keyTotalOrderKeysMap[key] == totalOrderKey && !tableKeySkewMap[tableIdx][key]) {
						tablesWithKeys.add(Tuple2.of(tableIdx, key));
					}
				}
			}
			// add dimensions per table if total order key is used in more than one table
			if (tablesWithKeys.size() >= 2) {
				for (final Tuple2<Integer, Integer> tableWithKey : tablesWithKeys) {
					// save used dimension
					// +1 because we need 0 for marking a replication dimension
					Utils.addMapListEntry(
						tableKeyDimensionMap, tableWithKey.f0, Tuple2.of(tableWithKey.f1, currentKey + 1));
					// save dimension key
					Utils.addMapListEntry(tableDimensionsMap, tableWithKey.f0, currentKey);
				}
				currentKey++;
			}
		}

		// R(x, y) x S(y, z) x T(z, x) + T.z skewed -> [x, y]
		// R(y) x S(y, z) x T(z) + T.z skewed -> [y, z']
		for (int totalOrderKey = 0; totalOrderKey < totalKeyOrderArity; ++totalOrderKey) {
			// for each table
			for (int tableIdx = 0; tableIdx < tableKeyTotalOrderKeysMap.length; ++tableIdx) {
				final int[] keyTotalOrderKeysMap = tableKeyTotalOrderKeysMap[tableIdx];
				// for each key of the table
				for (int key = 0; key < keyTotalOrderKeysMap.length; ++key) {
					// table contains total order key and it is skewed
					if (keyTotalOrderKeysMap[key] == totalOrderKey && tableKeySkewMap[tableIdx][key]) {
						List<Tuple2<Integer, Integer>> keys = tableKeyDimensionMap.get(tableIdx);
						// add dimension if there is no non-skewed key
						if (keys == null || keys.size() == 0) {
							// *-1 because it will be random partitioned
							// +1 because we need 0 for marking a replication dimension
							Utils.addMapListEntry(
								tableKeyDimensionMap, tableIdx, Tuple2.of(key, -1 * (currentKey + 1)));
							Utils.addMapListEntry(tableDimensionsMap, tableIdx, currentKey);
							currentKey++;
						}
					}
				}
			}
		}

		// generate table dimensions
		dimensions = currentKey;
		this.tableDimensionsMap = new int[tableKeyTotalOrderKeysMap.length][];
		for (int tableIdx = 0; tableIdx < tableKeyTotalOrderKeysMap.length; ++tableIdx) {
			this.tableDimensionsMap[tableIdx] = Utils.intListToArray(tableDimensionsMap.get(tableIdx));
		}

		// generate table key dimensions
		this.tableKeyDimensionMap = new int[tableKeyTotalOrderKeysMap.length][];
		for (final Map.Entry<Integer, List<Tuple2<Integer, Integer>>> entry : tableKeyDimensionMap.entrySet()) {
			final int[] keyDimensionMap = new int[dimensions];
			for (final Tuple2<Integer, Integer> keyMapping : entry.getValue()) {
				keyDimensionMap[keyMapping.f0] = keyMapping.f1;
			}
			this.tableKeyDimensionMap[entry.getKey()] = keyDimensionMap;
		}
	}

	private void calculateDimensionSizes() {
		this.bestConfiguration = new int[dimensions];
		// cache for variable number of factors (e.g. 8 = 8, 4, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1)
		final List<Integer> factors = new ArrayList<>();
		// indices pointing to elements in factors
		final int[] indicesCache = new int[dimensions];
		// cache for current combination
		final int[] combinationCache = new int[dimensions];
		for (int currentCubeSize = maxParallelism; currentCubeSize > 0; --currentCubeSize) {
			factors.clear();
			computeBoundedFactors(currentCubeSize, dimensions, factors);
			evaluateFactorCombinations(factors, dimensions, indicesCache, combinationCache, currentCubeSize);
		}
		// set best max parallelism
		int bestMaxParallelism = 1;
		for (int factor : bestConfiguration) {
			bestMaxParallelism *= factor;
		}
		this.bestMaxParallelism = bestMaxParallelism;
	}

	private void evaluateWorkload(final int[] configuration) {
		final double workload = computeWorkload(configuration);
		final int maxFactor;
		if (workload < bestWorkload) {
			this.bestWorkload = workload;
			System.arraycopy(configuration, 0, this.bestConfiguration, 0, configuration.length);
			this.bestMaxFactor = computeMaxFactor(configuration);
		} else if (workload == bestWorkload && bestMaxFactor < (maxFactor = computeMaxFactor(configuration))) {
			this.bestWorkload = workload;
			System.arraycopy(configuration, 0, this.bestConfiguration, 0, configuration.length);
			this.bestMaxFactor = maxFactor;
		}
	}

	private double computeWorkload(final int[] configuration) {
		double workload = 0.0;
		// for each relation
		for (int tableIdx = 0; tableIdx < cardinalities.length; ++tableIdx) {
			final int[] dimensionsMapping = tableDimensionsMap[tableIdx];
			int replication = 1;
			// for each dimension where this table is part of
			for (final int dimension : dimensionsMapping) {
				replication *= configuration[dimension];
			}
			workload += cardinalities[tableIdx] / replication;
		}
		return workload;
	}

	private static int computeMaxFactor(final int[] configuration) {
		int maxFactor = 0;
		for (int factor : configuration) {
			maxFactor = Math.max(maxFactor, factor);
		}
		return maxFactor;
	}

	private void evaluateFactorCombinations(
			final List<Integer> factors,
			final int maxFactors,
			final int[] indicesCache,
			final int[] combinationCache,
			final int maxProduct) {
		// source: https://stackoverflow.com/a/29914908/806430

		// first index sequence: 0, 1, 2, ...
		for (int i = 0; (indicesCache[i] = i) < maxFactors - 1;) {
			i++;
		}
		evaluateFactorCombination(factors, indicesCache, combinationCache, maxProduct);

		final int size = factors.size();
		while (true) {
			int i;
			// find position of item that can be incremented
			i = maxFactors - 1;
			while (i >= 0 && indicesCache[i] == size - maxFactors + i) {
				i--;
			}
			if (i < 0) {
				break;
			}

			// increment this item
			indicesCache[i]++;

			// fill up remaining items
			for (++i; i < maxFactors; ++i) {
				indicesCache[i] = indicesCache[i - 1] + 1;
			}

			evaluateFactorCombination(factors, indicesCache, combinationCache, maxProduct);
		}
	}

	private void evaluateFactorCombination(
			final List<Integer> factors,
			final int[] indices,
			final int[] combinationCache,
			final int maxProduct) {
		int prod = 1;
		for (int i = 0; i < indices.length; ++i) {
			combinationCache[i] = factors.get(indices[i]);
			prod *= combinationCache[i];
			// prevent unnecessary permutation
			if (prod > maxProduct) {
				return;
			}
		}
		evaluateFactorPermutations(combinationCache, combinationCache.length);
	}

	private void evaluateFactorPermutations(final int[] combinationCache, final int start) {
		// source: https://stackoverflow.com/a/36139335/806430

		if (start == combinationCache.length) {
			evaluateWorkload(combinationCache);
			return;
		}

		for (int i = start; i < combinationCache.length; ++i) {
			// swapping
			final int temp = combinationCache[i];
			combinationCache[i] = combinationCache[start];
			combinationCache[start] = temp;

			evaluateFactorPermutations(combinationCache, start + 1);

			// back swapping
			final int temp2 = combinationCache[i];
			combinationCache[i] = combinationCache[start];
			combinationCache[start] = temp2;
		}
	}

	private static void computeBoundedFactors(final int n, final int maxFactors, final List<Integer> output) {
		// source: https://softwareengineering.stackexchange.com/a/264072

		final int maxD = (int) Math.sqrt(n);
		for (int i = 1; i <= maxD; ++i) {
			if (n % i == 0) {
				// add each factor multiple times if the product is still a factor of n
				int prod = i;
				for (int j = 0; j < maxFactors && n % prod == 0; ++j) {
					// add the last factor only if it can produce n
					// valid: 8 => 2 x 2 x 2
					// invalid: 513 => 3 x 3 x 3
					// invalid: 63 => 7 x 7 ...
					if (j != maxFactors - 1 || prod == n) {
						output.add(i);
						prod *= i;
					}
				}
				final int d = n / i;
				if (d != i) {
					output.add(d);
				}
			}
		}
	}

	private void calculateHyperCubeMap() {
		final TreeMap<Integer, CubePosition> cellsWithGaps = new TreeMap<>();
		// handle special case 1 dimension
		if (bestMaxFactor == 1) {
			for (int i = 0; i < bestConfiguration[0]; ++i) {
				final CubePosition cubePosition = new CubePosition(1);
				cubePosition.coords[0] = i;
				cellsWithGaps.put(i, cubePosition);
			}
		} else {
			// round up to next power of 2
			final int curveBits = 32 - Integer.numberOfLeadingZeros(bestMaxFactor - 1);
			final HilbertCurve curve = HilbertCurve.bits(curveBits).dimensions(dimensions);
			collectHilbertCells(curve, 0, new int[dimensions], cellsWithGaps);
		}

		// adapt indices to actual cube size
		hyperCubeMap = new HashMap<>();
		int idx = 0;
		for (Map.Entry<Integer, CubePosition> entry : cellsWithGaps.entrySet()) {
			hyperCubeMap.put(entry.getValue(), idx++);
		}
	}

	private void collectHilbertCells(
			final HilbertCurve curve,
			final int dimension,
			final int[] counters,
			final Map<Integer, CubePosition> cells) {
		if (dimension == counters.length) {
			final CubePosition cell = new CubePosition(dimensions);
			System.arraycopy(counters, 0, cell.coords, 0, dimensions);

			final int index = curve.index(Utils.intToLongArray(cell.coords)).intValue();
			cells.put(index, cell);
		} else {
			for (counters[dimension] = 0; counters[dimension] < bestConfiguration[dimension]; counters[dimension]++) {
				collectHilbertCells(curve, dimension + 1, counters, cells);
			}
		}
	}

	private void calculatePosCellsMap() {
		tablePosGroupMap = new ArrayList<>();

		int cellGroupId = 0;
		final List<int[]> cellGroupList = new ArrayList<>();

		// for each table
		for (int tableIdx = 0; tableIdx < cardinalities.length; ++tableIdx) {
			final int[] keyDimensionMap = tableKeyDimensionMap[tableIdx];
			final Map<CubePosition, CubeGroup> hyperCubePosCellsMap = new HashMap<>(maxParallelism * 2);

			final Map<CubePosition, List<Integer>> hyperCubePosCellsListMap = new HashMap<>(maxParallelism * 2);
			collectHyperCubeCells(0, new int[dimensions], keyDimensionMap, hyperCubePosCellsListMap);

			// convert map
			for (final Map.Entry<CubePosition, List<Integer>> entry : hyperCubePosCellsListMap.entrySet()) {
				final int[] cells = Utils.intListToArray(entry.getValue());
				hyperCubePosCellsMap.put(entry.getKey(), new CubeGroup(cellGroupId++, cells));
				cellGroupList.add(cells);
			}

			tablePosGroupMap.add(hyperCubePosCellsMap);
		}

		// create group mapping
		groupCellsMap = new int[cellGroupList.size()][];
		for (int i = 0; i < groupCellsMap.length; ++i) {
			groupCellsMap[i] = cellGroupList.get(i);
		}
	}

	private void collectHyperCubeCells(
			final int dimension,
			final int[] counters,
			final int[] keyDimensionMap,
			final Map<CubePosition, List<Integer>> hyperCubePosCellsMap) {
		if (dimension == counters.length) {
			// set all coordinates
			final CubePosition posWithRepl = new CubePosition(dimensions);
			System.arraycopy(counters, 0, posWithRepl.coords, 0, dimensions);

			// overwrite replication dimensions
			for (int dim = 0; dim < dimensions; ++dim) {
				// check if key is partitioned (random or hash)
				if (!Ints.contains(keyDimensionMap, dim + 1) && !Ints.contains(keyDimensionMap, (dim + 1) * -1)) {
					posWithRepl.coords[dim] = Integer.MAX_VALUE;
				}
			}

			// insert into map
			final List<Integer> cells;
			if (hyperCubePosCellsMap.containsKey(posWithRepl)) {
				cells = hyperCubePosCellsMap.get(posWithRepl);
			} else {
				cells = new ArrayList<>();
				hyperCubePosCellsMap.put(posWithRepl, cells);
			}

			// determine cube cell for current coordinates
			final CubePosition pos = new CubePosition(dimensions);
			System.arraycopy(counters, 0, pos.coords, 0, dimensions);
			cells.add(hyperCubeMap.get(pos));
		} else {
			// recursive dimension generation
			for (counters[dimension] = 0; counters[dimension] < bestConfiguration[dimension]; counters[dimension]++) {
				collectHyperCubeCells(dimension + 1, counters, keyDimensionMap, hyperCubePosCellsMap);
			}
		}
	}

	public int getBestMaxParallelism() {
		return bestMaxParallelism;
	}

	public int getDimensionCount() {
		return dimensions;
	}

	public int[] getDimensionSizes() {
		return bestConfiguration;
	}

	public int[][] getTableKeyDimensionMap() {
		return tableKeyDimensionMap;
	}

	public List<Map<CubePosition, CubeGroup>> getTablePosGroupMap() {
		return tablePosGroupMap;
	}

	public int[][] getGroupCellsMap() {
		return groupCellsMap;
	}
}
