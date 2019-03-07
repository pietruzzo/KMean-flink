package org.pietro.flink.utils;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import org.pietro.flink.Centroid;
import org.pietro.flink.Point;

import java.util.Arrays;

public class DatasetIO {

    /**
     * import datasets from files given by arguments
     * @param params -p, -c parameters
     * @param env execution environement
     * @return Tuple2 containing datasets for Points and Centroids
     */
    public static Tuple2<DataSet<Point>,DataSet<Centroid>> importFromFile(ParameterTool params, ExecutionEnvironment env){
        DataSet<Centroid> centroids;
        DataSet<Point> points;

        if (params.has("c")) {
            centroids = env.readTextFile(params.get("c"))
                    .flatMap(new CentroidSplitter());
        } else {
            System.out.println("no -c specified -> use debug dataset");
            centroids = DebugDataset.getDefaultCentroidDataSet(env);
        }

        if (params.has("p")) {
            points = env.readTextFile(params.get("p"))
                    .flatMap(new PointSplitter());
        } else {
            System.out.println("no -p specified -> use debug dataset");
            points = DebugDataset.getDefaultPointDataSet(env);
        }
        return new Tuple2<>(points, centroids);
    }

    public static void printResults(ParameterTool params, DataSet<Tuple2<Integer, Point>> clusteredPoints, ExecutionEnvironment env ) throws Exception {
        if (params.has("o")) {
            clusteredPoints.writeAsCsv(params.get("o"), "\n", "\t");
            env.execute("K Clustering");

        } else {
            System.out.println(" -o to specify output path");
            clusteredPoints.print();
        }
    }

    static Double[] extractDimensions (String[] s){

        Double[] dimensions = new Double[s.length];
        for (int i = 0; i < s.length; i++) {
            dimensions[i] = Double.parseDouble(s[i]);
        }
        return dimensions;
    }
}

/**
 * id dim1 dim2 dim3... -> centroids
 */
class CentroidSplitter implements FlatMapFunction<String, Centroid> {

    @Override
    public void flatMap(String s, Collector<Centroid> collector) throws Exception {
        String[] splitted = s.split(" ");
        if(splitted.length !=0)
            collector.collect(new Centroid(Integer.parseInt(splitted[0]), DatasetIO.extractDimensions(Arrays.copyOfRange(splitted, 1, splitted.length))));
    }
}

/**
 * dim1 dim2 dim3... -> points
 */
class PointSplitter implements FlatMapFunction<String, Point> {

    @Override
    public void flatMap(String s, Collector<Point> collector) throws Exception {
        String[] splitted = s.split(" ");
        if(splitted.length !=0)
            collector.collect(new Point(DatasetIO.extractDimensions(splitted)));
    }
}