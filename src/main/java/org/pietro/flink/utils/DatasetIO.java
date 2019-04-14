package org.pietro.flink.utils;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import org.pietro.flink.Centroid;
import org.pietro.flink.Point;

import java.io.File;
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
        String separator;

        if(params.has("s")) separator = params.get("s");
        else separator = " ";
        if (params.has("c")) {
            centroids = env.readTextFile(params.get("c"))
                    .flatMap(new CentroidSplitter(separator));
        } else {
            System.out.println("no -c specified -> use debug dataset");
            centroids = DebugDataset.getDefaultCentroidDataSet(env);
        }

        if (params.has("p")) {
            points = env.readTextFile(params.get("p"))
                    .flatMap(new PointSplitter(separator));
        } else {
            System.out.println("no -p specified -> use debug dataset");
            points = DebugDataset.getDefaultPointDataSet(env);
        }
        return new Tuple2<>(points, centroids);
    }

    public static void printResults(ParameterTool params, DataSet<Tuple2<Integer, Point>> clusteredPoints, ExecutionEnvironment env ) throws Exception {
        if (params.has("o")) {
            File index = new File(params.get("o"));
            String[]entries = index.list();
            for(String s: entries){
                File currentFile = new File(index.getPath(),s);
                currentFile.delete();
            }
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

    private String separator;

    CentroidSplitter(String separator){
        super();
        this.separator = separator;
    }
    @Override
    public void flatMap(String s, Collector<Centroid> collector) throws Exception {
        String[] splitted = s.split(separator);
        if(splitted.length !=0)
            collector.collect(new Centroid(Integer.parseInt(splitted[0]), DatasetIO.extractDimensions(Arrays.copyOfRange(splitted, 1, splitted.length))));
    }
}

/**
 * dim1 dim2 dim3... -> points
 */
class PointSplitter implements FlatMapFunction<String, Point> {

    private String separator;

    PointSplitter(String separator){
        super();
        this.separator = separator;
    }
    @Override
    public void flatMap(String s, Collector<Point> collector) throws Exception {
        String[] splitted = s.split(separator);
        if(splitted.length !=0)
            collector.collect(new Point(DatasetIO.extractDimensions(splitted)));
    }
}