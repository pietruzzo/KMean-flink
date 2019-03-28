package org.pietro.flink;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.pietro.flink.utils.DatasetIO;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Collection;


/**
 * K-Clustering Flink program
 * args:
 *  -c centroid absolute path
 *  -p points absolute path
 *  -n max number of iterations
 *  -o output absolute path
 *  -t threshold (double)
 *  -s separator (" " if not defined)
 *  --convergence-condition
 */
public class KMeanClustering {

    private static int MAX_DEFAULT_ITERATIONS = 50;
    private static double DEFAULT_THRESHOLD = 0.0;

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);

        // get execution environment and send params to the web interface
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        // get datasets
        Tuple2<DataSet<Point>, DataSet<Centroid>> sets = DatasetIO.importFromFile(params, env);
        DataSet<Point> points = sets.f0;
        DataSet<Centroid> centroids = sets.f1;

        //get close condition and threshold
        Boolean convergence = params.getBoolean("t", true);
        Double threshold = params.getDouble("t", DEFAULT_THRESHOLD);

        // set iteration
        IterativeDataSet<Centroid> loop = centroids.iterate(params.getInt("n", MAX_DEFAULT_ITERATIONS));

        /**
         * For each point: compute closest centroid
         * For each centroid: sum coordinates and count number of nodes, then average
         */
        DataSet<Centroid> newCentroids = points
                .map(new nearestCentroid()).withBroadcastSet(loop, "centroids").name("nearestCentroid")
                .map(new GetCounter()).name("addCounter")
                .groupBy(0).reduce(new CentroidAccumulator())
                .map(new AVG()).name("AverageNewCentroids");

        //region: Close Condition
        DataSet<Centroid> finalCentroids;
        if (convergence) {
            // Join centroid dataset with old one on id (cluster)
            DataSet<Tuple2<Centroid, Centroid>> compareSet = newCentroids
                    .join(loop)
                    .where("id")
                    .equalTo("id");

            //get non converged centroids
            DataSet<Centroid> terminationSet = compareSet
                    .flatMap(new isConverged(threshold)).name("findNotConverged");

            // feed back centroids and stop if all centroids are converged
            finalCentroids = loop.closeWith(newCentroids, terminationSet);
        } else {
            finalCentroids = loop.closeWith(newCentroids);
        }
        //endregion

        // assign points to final clusters or perform another iteration
        DataSet<Tuple2<Integer, Point>> clusteredPoints = points
                .map(new nearestCentroid()).withBroadcastSet(finalCentroids, "centroids");

        DatasetIO.printResults(params, clusteredPoints, env);

    }
}

/** Appends a count variable to the tuple. */
class GetCounter implements MapFunction<Tuple2<Integer, Point>, Tuple3<Integer, Point, Long>> {

    @Override
    public Tuple3<Integer, Point, Long> map(Tuple2<Integer, Point> t) {
        return new Tuple3<>(t.f0, t.f1, 1L);
    }
}

/**
 * Divide Point dimensions for the Long scalar
 */
class AVG implements MapFunction<Tuple3<Integer, Point, Long>, Centroid> {

    /**
     * for each Point
     * @param val
     * @return centroid
     */
    @Override
    public Centroid map(Tuple3<Integer, Point, Long> val) {
        return new Centroid(val.f0, val.f1.divideScalar(val.f2));
    }
}

/**
 * Find closest centroid for a data Point
 * Uses Broadcast variable "centroids"
 */
class nearestCentroid extends RichMapFunction<Point, Tuple2<Integer, Point>> {
    private Collection<Centroid> centroids;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
    }

    /**
     * @param p current point
     * @return closest centroid, with distance
     * @throws Exception
     */
    @Override
    public Tuple2<Integer, Point> map(Point p) throws Exception {

        double minDist = -1;
        int minDistCentroidID = -1;

        for (Centroid centroid : centroids) {
            double distance = p.distance(centroid);
            if (distance < minDist || minDist == -1) {
                minDistCentroidID = centroid.id;
                minDist = distance;
            }
        }

        return new Tuple2<>(minDistCentroidID, p);
    }
}

class CentroidAccumulator implements ReduceFunction<Tuple3<Integer, Point, Long>> {

    /**
     * Sum points dimensions and count points summed till now
     * @param val1
     * @param val2
     * @return
     */
    @Override
    public Tuple3<Integer, Point, Long> reduce(Tuple3<Integer, Point, Long> val1, Tuple3<Integer, Point, Long> val2) {
        return new Tuple3<>(val1.f0, val1.f1.addPoint(val2.f1), val1.f2 + val2.f2);
    }
}

/**
 * Check if the clusters are converged given a threashold
 * collect only non-convergent clusters
 */
class isConverged implements FlatMapFunction<Tuple2<Centroid, Centroid>, Centroid> {

    private double threshold;

    public isConverged(double threshold) {
        this.threshold = threshold;
    }

    @Override
    public void flatMap(Tuple2<Centroid, Centroid> pairCentroids, Collector<Centroid> col)  {
        if (pairCentroids.f0.distance(pairCentroids.f1) >= Math.pow(threshold, 2)) {
            col.collect(pairCentroids.f0);
        }
    }
}