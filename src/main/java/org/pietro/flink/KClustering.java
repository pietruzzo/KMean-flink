package org.pietro.flink;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.awt.*;
import java.util.ArrayList;
import java.util.List;

public class KClustering {

    private static final String INPUT_VALUES = "sample.cvs";
    private static final String INPUT_CANTROIDS = "centroids.csv";
    private static final String OUTPUT_FILE = "output.csv";

    private static int maxIterations = 100;
    private static int keyPosition = 0;

    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        // get centroids
        DataSet<Tuple3<Integer, Double[], Integer>> initialDeltaSet = env.readTextFile(INPUT_CANTROIDS).flatMap(new Splitter());

        //Open Stream from CSV file
        DataSet<Tuple3<Integer, Double[], Integer>> initialSolutionSet = env.readTextFile(INPUT_VALUES).flatMap(new Splitter());


        //Tranformations

        DeltaIteration<Tuple3<Integer, Double[], Integer>, Tuple3<Integer, Double[], Integer>> iteration = initialSolutionSet
                .iterateDelta(initialDeltaSet, maxIterations, keyPosition);

        //Assign each node to nearest cluster
        DataSet<Tuple3<Integer, Double[], Integer>> newSolutionSet = iteration
                .getSolutionSet()
                .cross(iteration.getWorkset())
                .with(new CalculateDistanceCross())
                .groupBy((KeySelector<Tuple3<Double, Integer, Tuple3<Integer, Double[], Integer>>, Integer>) tuple -> tuple.f2.f0)//Group by id of node
                .minBy(0)//get min distance (Get for each node the nearest centroid)
                .map(new AssignToCluster());


        //From SUM to AVG
        DataSet<Tuple3<Integer, Double[], Integer>> newCentroids = iteration
                .getSolutionSet()
                .map(new CountedPoint())
                .groupBy((KeySelector<Tuple2<Tuple3<Integer, Double[], Integer>, Integer>, Integer>) tuple -> tuple.f0.f2) //Raggruppati per cluster
                .reduce(new PointsDimensionSum())
                .map(new AVG())
                ;

        //New Workset
        DataSet<Tuple3<Integer, Double[], Integer>> newWorkset = iteration
                .getWorkset()
                .join(newCentroids).where(2).equalTo(2)
                .map(new MapWorksetInformations())
                ;

        //Delta Workset
        DataSet<Tuple3<Integer, Double[], Integer>> delta = iteration
                .getWorkset()
                .join(newWorkset).where(2).equalTo(2)
                .filter((FilterFunction<Tuple2<Tuple3<Integer, Double[], Integer>, Tuple3<Integer, Double[], Integer>>>) t1 -> {
                    for (int i = 0; i < t1.f0.f1.length; i++) {
                        if (t1.f0.f1[i] != t1.f1.f1[i]) return true;
                    }
                    return false;
                })
                .map(new MapFunction<Tuple2<Tuple3<Integer, Double[], Integer>, Tuple3<Integer, Double[], Integer>>, Tuple1<Boolean>>() {
                    @Override
                    public Tuple1<Boolean> map(Tuple2<Tuple3<Integer, Double[], Integer>, Tuple3<Integer, Double[], Integer>> tuple3Tuple3Tuple2) throws Exception {
                        return new Tuple1<>(true);
                    }
                })
                .first(1)
                .cross(iteration.getWorkset())
                .project(01);

        iteration.closeWith(newSolutionSet, delta)
        .writeAsCsv(OUTPUT_FILE);
        // execute program
        env.execute();
    }

}


/**
 * id, dim1 dim2 dim3
 * id, dim1 dim2 dim3
 * id, dim1 dim2 dim3... points
 *
 * id, dim1 dim2 dim3, cluster -> centroids
 */
class Splitter implements FlatMapFunction<String, Tuple3<Integer, Double[], Integer>> {


    @Override
    public void flatMap(String s, Collector<Tuple3<Integer, Double[], Integer>> collector) throws Exception {
        List<Double> dimensions = new ArrayList<>();
        String[] splitted = s.split(",");
        for (String word : splitted[1].split(" ")) {
            dimensions.add(Double.parseDouble(word));
        }
        if (splitted.length==2) //Point
            collector.collect(new Tuple3<>(Integer.parseInt(splitted[0]), (Double[]) dimensions.toArray(), -1));
        else if (splitted.length == 3) //Centroid
            collector.collect(new Tuple3<>(Integer.parseInt(splitted[0]), (Double[]) dimensions.toArray(), Integer.parseInt(splitted[2])));
        else System.out.println(s + " is bad formatted, thus ignored");
    }
}

/**
 * Return Tuple3<Distance, Cluster, Point>
 * TODO: verify that p1 is point and p2 is centroid
 */
class CalculateDistanceCross implements CrossFunction<Tuple3<Integer, Double[], Integer>, Tuple3<Integer, Double[], Integer>, Tuple3<Double, Integer, Tuple3<Integer, Double[], Integer>>>{

    @Override
    public Tuple3<Double, Integer, Tuple3<Integer, Double[], Integer>> cross(Tuple3<Integer, Double[], Integer> p1, Tuple3<Integer, Double[], Integer> p2) throws Exception {
        if (p1.f1.length!= p2.f1.length) throw new IllegalArgumentException("Different number of dimensions");

        double sum = 0;
        for (int i = 0; i < p1.f1.length; i++) {
            sum = sum + Math.pow(p1.f1[i] - p2.f1[i] , 2);
        }
        return new Tuple3<>(Math.sqrt(sum), p2.f2, p1);
    }
}

class AssignToCluster implements MapFunction<Tuple3<Double, Integer, Tuple3<Integer, Double[], Integer>>, Tuple3<Integer, Double[], Integer>>{

    @Override
    public Tuple3<Integer, Double[], Integer> map(Tuple3<Double, Integer, Tuple3<Integer, Double[], Integer>> point) throws Exception {
        return new Tuple3<>(point.f2.f0, point.f2.f1, point.f1);
    }
}

/**
 * <IN, ACC, OUT>
 *     IN: Point Tuple3<ID, dimensions, Cluster>
 *     ACC: Tuple3<Number, dimensions, Cluster>
 *     OUT: Tuple2<Dimensions, Cluster>
 */
class CentroidDimensionAvg implements AggregateFunction<Tuple3<Integer, Double[], Integer>, Tuple3<Integer, Double[], Integer>, Tuple2<Double[], Integer> > {

    @Override
    public Tuple3<Integer, Double[], Integer> createAccumulator() {
        Double[] t = new Double[2];
        return new Tuple3<>(0, new Double[1], null);
    }

    @Override
    public Tuple3<Integer, Double[], Integer> add(Tuple3<Integer, Double[], Integer> point, Tuple3<Integer, Double[], Integer> acc) {
        if ( acc.f2 != null ){
            Double[] dim = acc.f1.clone();
            for (int i = 0; i < point.f1.length; i++) {
                dim[i]=dim[i]+ point.f1[i];
            }
            return new Tuple3<>(acc.f0 + 1, dim, acc.f2);
        } else {//first point
            return new Tuple3<>(1, point.f1, point.f2);
        }
    }

    @Override
    public Tuple2<Double[], Integer> getResult(Tuple3<Integer, Double[], Integer> acc) {
        for (int i = 0; i < acc.f1.length; i++) {
            acc.f1[i] = acc.f1[i] / acc.f0;
        }
        return new Tuple2<>(acc.f1, acc.f2);
    }

    @Override
    public Tuple3<Integer, Double[], Integer> merge(Tuple3<Integer, Double[], Integer> acc2, Tuple3<Integer, Double[], Integer> acc1) {
        for (int i = 0; i < acc1.f1.length; i++) {
            acc1.f1[i] = acc1.f1[i] + acc2.f1[i];
        }
        acc1.f0 = acc1.f0 + acc2.f0;
        return acc1;
    }


}


/**
 * Tuple2<Point, numOfPoints>
 */
class PointsDimensionSum implements ReduceFunction<Tuple2<Tuple3<Integer, Double[], Integer>, Integer>>{


    @Override
    public Tuple2<Tuple3<Integer, Double[], Integer>, Integer> reduce(Tuple2<Tuple3<Integer, Double[], Integer>, Integer> t2, Tuple2<Tuple3<Integer, Double[], Integer>, Integer> t1) throws Exception {
        Double[] d1 = t1.f0.f1;
        Double[] d2 = t2.f0.f1;
        for (int i = 0; i < d1.length; i++) {
            d1[i] = d1[i] + d2[i];
        }
        return new Tuple2<>(new Tuple3<>(t1.f0.f0, d1, t1.f0.f2), t1.f1 + t2.f1);
    }
}

class CountedPoint implements MapFunction<Tuple3<Integer, Double[], Integer>, Tuple2<Tuple3<Integer, Double[], Integer>, Integer>> {

    @Override
    public Tuple2<Tuple3<Integer, Double[], Integer>, Integer> map(Tuple3<Integer, Double[], Integer> point) throws Exception {
        return new Tuple2<>(point, 1);
    }
}

class AVG implements MapFunction<Tuple2<Tuple3<Integer, Double[], Integer>, Integer>, Tuple3<Integer, Double[], Integer>>{

    @Override
    public Tuple3<Integer, Double[], Integer> map(Tuple2<Tuple3<Integer, Double[], Integer>, Integer> tuple) throws Exception {
        for (int i = 0; i < tuple.f0.f1.length; i++) {
            tuple.f0.f1[i] = tuple.f0.f1[i] / tuple.f1;
        }
        return tuple.f0;
    }
}

class MapWorksetInformations implements MapFunction<Tuple2<Tuple3<Integer, Double[], Integer>, Tuple3<Integer, Double[], Integer>>, Tuple3<Integer, Double[], Integer>>{

    @Override
    public Tuple3<Integer, Double[], Integer> map(Tuple2<Tuple3<Integer, Double[], Integer>, Tuple3<Integer, Double[], Integer>> t) throws Exception {
        return new Tuple3<>(t.f1.f0, t.f0.f1, t.f0.f2);
    }
}