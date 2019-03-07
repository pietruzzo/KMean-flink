package org.pietro.flink.deltaVersion;

import java.io.Serializable;
import java.util.List;

/**
 * TODO dimension va portato a double
 */
public class Point implements Serializable {
    
    private Integer[] dimension;
    private Integer cluster;

    public Point(){};

    public Point(List<Integer> dimension){
        this.dimension = (Integer[]) dimension.toArray();
        cluster = null;
    }

    public Point(List<Integer> dimension, int cluster){
        this.dimension = (Integer[]) dimension.toArray();
        this.cluster = cluster;
    }

    public int getNumDimensions() {
        return dimension.length;
    }

    public Integer[] getDimension() {
        return dimension.clone();
    }

    public void setDimension(List<Integer> dimension) {
        this.dimension = (Integer[]) dimension.toArray();
    }

    public void setDimension(Integer[] dimension) {
        this.dimension = (Integer[]) dimension;
    }

    public Integer getCluster(){
        return cluster;
    }

    public void setCluster(Integer cluster){
        this.cluster = cluster;
    }


    public double getDistance(Point p1) {

        if (p1.getNumDimensions()!= this.getNumDimensions()) throw new IllegalArgumentException("Different number of dimensions");

        double sum = 0;
        for (int i = 0; i < getNumDimensions(); i++) {
            sum = sum + Math.pow(p1.getDimension()[i] - this.getDimension()[i] , 2);
        }
        return Math.sqrt(sum);
    }

}
