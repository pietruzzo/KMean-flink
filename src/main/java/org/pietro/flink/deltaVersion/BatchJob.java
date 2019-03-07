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

package org.pietro.flink.deltaVersion;

import org.apache.commons.math3.exception.DimensionMismatchException;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author pietro
 */
public class BatchJob {

	static final String INPUT_VALUES = "sample.cvs";
	static final String INPUT_CANTROIDS = "centroids.csv";
	static final String OUTPUT_FILE = "output.csv";

	public static void main(String[] args) throws Exception {

		int maxIterations = 100;
		int keyPosition = 0;

		// set up the batch execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


		// get centroids
		DataSet<Point> centroids = env.readTextFile(INPUT_CANTROIDS).flatMap(new LineSplitter());

		//Open Stream from CSV file
		DataSet<Point> input = env.readTextFile(INPUT_VALUES).flatMap(new LineSplitter());

		//Tranformations
		//DataSet<Point> tranformed = input.map(new NearestCentroid());

		DataSet<Tuple1<Point>> initialSolutionSet = input.map(new ToTuple1Map());

		DataSet<Tuple1<Point>> initialDeltaSet = centroids.map(new ToTuple1Map());


		DeltaIteration<Tuple1<Point>, Tuple1<Point>> iteration = initialSolutionSet
				.iterateDelta(initialDeltaSet, maxIterations, keyPosition, 0, 0);

		DataSet<Tuple1<Point>> newSolutionSet = iteration
				.getSolutionSet()
				.map((MapFunction<Tuple1<Point>, Point>) point -> point.f0)
				.cross(iteration.getWorkset().map((MapFunction<Tuple1<Point>, Point>) point -> point.f0))
				.with(new CentroidDistanceCross())
				.groupBy(0)
				.reduce(new MinReduceFunction()) //Esiste una funzione apposita per il minimo!!
				.project(100);

		//From SUM to AVG
		DataSet<Tuple1<Point>> newWorkset = iteration.getSolutionSet()
				.map(new MapFunction<Tuple1<Point>, Tuple2<Point, Integer>>() {
					@Override
					public Tuple2<Point, Integer> map(Tuple1<Point> point) throws Exception {
						return new Tuple2<>(point.f0, 1);
					}
				})
				.groupBy("f0.cluster")
				.reduce(new CentroidCalculationReduce())
				.map((MapFunction<Tuple2<Point, Integer>, Tuple1<Point>>) point -> {
					List<Integer> dims = new ArrayList<>();
					for (int i = 0; i < point.f0.getNumDimensions(); i++) {
						dims.add(point.f0.getDimension()[i] / point.f1);
					}
					point.f0.setDimension(dims);
					return new Tuple1(point.f0);
				});


		//TODO: Add termination criterio
		//TODO: Ad Performance
		iteration.closeWith(newSolutionSet, newWorkset)
				.writeAsCsv(OUTPUT_FILE);

		// execute program
		env.execute("Flink Batch Java Centroids");
	}
}


class LineSplitter implements FlatMapFunction<String, Point> {

	@Override
	public void flatMap(String line, Collector<Point> out) {
		List<Integer> dimensions = new ArrayList<>();
		for (String word : line.split(" ")) {
			dimensions.add(Integer.parseInt(word));
		}
		out.collect(new Point(dimensions));
	}
}

/*
class NearestCentroid implements MapFunction<Point, Point>{


	@Override
	public Point map(Point point) throws Exception {

		Tuple2<Point, Double> min = null;
		for (Point c: BatchJob.centroids) {
			Double distance = point.getDistance(c);
			if(min == null || min.f1>distance)
				min = new Tuple2<Point, Double>(c, distance);
		}
		point.setCluster(min.f0.getCluster());
		return point;
	}
}
*/

/**
 * TODO Class Centroid Calculation
 * avg each dimension of points
 * return the centroid
 */
class CentroidCalculation implements AggregateFunction<Point, Tuple2<Point, Integer>, Point>{


	@Override
	public Tuple2<Point, Integer> createAccumulator() {
		return new Tuple2<>(new Point(new ArrayList<>()), 0);
	}

	@Override
	public Tuple2<Point, Integer> add(Point point, Tuple2<Point, Integer> acc) {
		List<Integer> dimensionsPoint = Arrays.asList(point.getDimension());
		List<Integer> dimensionsAcc = Arrays.asList(acc.f0.getDimension());
		try {
			acc.f1 = acc.f1 + 1;
			for (int i = 0; i < dimensionsPoint.size(); i++) {
				dimensionsAcc.set(i, dimensionsAcc.get(i) + dimensionsPoint.get(i));
			}
		} catch (IndexOutOfBoundsException e){
			acc.f1 = 1;
			if ( dimensionsAcc.size() > 0 ) throw new RuntimeException(acc.toString()+ " should be of zero elements or of the same size of the Point " + point.toString());
			for (int i = 0; i < dimensionsPoint.size(); i++) {
				dimensionsAcc.add(dimensionsPoint.get(i));
			}
		} finally {
			acc.f0.setDimension(dimensionsAcc);
			acc.f0.setCluster(point.getCluster());
			return acc;
		}
	}

	@Override
	public Point getResult(Tuple2<Point, Integer> acc) {
		List<Integer> dimensionsAcc = Arrays.asList(acc.f0.getDimension());
		for (int i = 0; i < dimensionsAcc.size(); i++) {
			dimensionsAcc.set(i, dimensionsAcc.get(i)/ acc.f1);
		}
		acc.f0.setDimension(dimensionsAcc);
		return acc.f0;
	}

	@Override
	public Tuple2<Point, Integer> merge(Tuple2<Point, Integer> acc2, Tuple2<Point, Integer> acc1) {
		if (acc2.f0.getNumDimensions() != acc1.f0.getNumDimensions()) throw new IndexOutOfBoundsException("counter 1 has " + acc1.f0.getNumDimensions() + " dimensions, while counter 2 has " + acc2.f0.getNumDimensions());
		List<Integer> newDim = new ArrayList<>();
		for (int i = 0; i < acc1.f0.getNumDimensions(); i++) {
			Integer d = acc1.f0.getDimension()[i] + acc2.f0.getDimension()[i];
			newDim.add(d);
		}
		return new Tuple2<Point, Integer>(new Point(newDim, acc1.f0.getCluster()), acc1.f1 + acc2.f1);
	}
}

/**
 * Avg each dimension of Point and return the Centroid
 */
class CentroidCalculationReduce extends RichReduceFunction<Tuple2<Point, Integer>> {

	/**
	 *
	 * @param pointIntegerTuple2
	 * @param t1
	 * @return
	 * @throws Exception
	 */
	@Override
	public Tuple2<Point, Integer> reduce(Tuple2<Point, Integer> pointIntegerTuple2, Tuple2<Point, Integer> t1) throws Exception {
		if (pointIntegerTuple2.f0.getNumDimensions() != t1.f0.getNumDimensions())
			throw new DimensionMismatchException(pointIntegerTuple2.f0.getNumDimensions(), t1.f0.getNumDimensions());
		List<Integer> dims = new ArrayList<>();
		for (int i = 0; i < pointIntegerTuple2.f0.getNumDimensions(); i++)
			dims.add(pointIntegerTuple2.f0.getDimension()[i] + t1.f0.getDimension()[i]);
		t1.f0.setDimension(dims);
		t1.f1 = t1.f1 + pointIntegerTuple2.f1;
		return t1;
	}
}


class CentroidDistanceCross implements CrossFunction<Point, Point, Tuple3<Point, Point, Double>> {

	@Override
	public Tuple3<Point, Point, Double> cross(Point point, Point centroid) {
		// compute Euclidean distance of coordinates
		return new Tuple3<>(point, centroid, point.getDistance(centroid));
	}
}

class MinReduceFunction implements ReduceFunction<Tuple3<Point, Point, Double>>{

	@Override
	public Tuple3<Point, Point, Double> reduce(Tuple3<Point, Point, Double> p1, Tuple3<Point, Point, Double> p2) throws Exception {
		if (p1.f2 > p2.f2)
			return p2;
		return p1;
	}
}

class ToTuple1Map implements MapFunction<Point, Tuple1<Point>>{

	@Override
	public Tuple1<Point> map(Point obj) throws Exception {
		return new Tuple1(obj);
	}
}