package org.pietro.flink;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.pietro.flink.utils.DebugDataset;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import java.util.Collection;


/**
 * K-Clustering Flink program
 */
public class KMeans {

    private static int MAX_DEFAULT_ITERATIONS = 50;

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);

        // get execution environment and send params to the web interface
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        // get datasets
        DataSet<Point> points = getPointDataSet(params, env);
        DataSet<Centroid> centroids = getCentroidDataSet(params, env);

        // set iteration
        IterativeDataSet<Centroid> loop = centroids.iterate(params.getInt("num-iterations", MAX_DEFAULT_ITERATIONS));

        /**
         * For each point: compute closest centroid
         * For each centroid: sum coordinates and count number of nodes, then average
         */
        DataSet<Centroid> newCentroids = points
                .map(new nearestCentroid()).withBroadcastSet(loop, "centroids")
                .map(new GetCounter())
                .groupBy(0).reduce(new CentroidAccumulator())
                .map(new AVG());

        // TODO: Close condition
        DataSet<Centroid> finalCentroids = loop.closeWith(newCentroids);

        DataSet<Tuple2<Integer, Point>> clusteredPoints = points
                // assign points to final clusters
                .map(new nearestCentroid()).withBroadcastSet(finalCentroids, "centroids");

        // TODO: emit result, use a specific function in Utils
        if (params.has("output")) {
            clusteredPoints.writeAsCsv(params.get("output"), "\n", " ");

            // since file sinks are lazy, we trigger the execution explicitly
            env.execute("KMeans Example");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            clusteredPoints.print();
        }
    }



    private static DataSet<Centroid> getCentroidDataSet(ParameterTool params, ExecutionEnvironment env) {
        DataSet<Centroid> centroids;
        if (params.has("centroids")) {
            centroids = env.readCsvFile(params.get("centroids"))
                    .fieldDelimiter(" ")
                    .pojoType(Centroid.class, "id", "x", "y");
        } else {
            System.out.println("Executing K-Means example with default centroid data set.");
            System.out.println("Use --centroids to specify file input.");
            centroids = DebugDataset.getDefaultCentroidDataSet(env);
        }
        return centroids;
    }

    private static DataSet<Point> getPointDataSet(ParameterTool params, ExecutionEnvironment env) {
        DataSet<Point> points;
        if (params.has("points")) {
            // read points from CSV file
            points = env.readCsvFile(params.get("points"))
                    .fieldDelimiter(" ")
                    .pojoType(Point.class, "x", "y");
        } else {
            System.out.println("Executing K-Means example with default point data set.");
            System.out.println("Use --points to specify file input.");
            points = DebugDataset.getDefaultPointDataSet(env);
        }
        return points;
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