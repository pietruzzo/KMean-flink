package org.pietro.flink;

import java.io.Serializable;

/**
 * Multidimensional Point
 * POJO object
 */
public class Point implements Serializable {

    public Double[] dims;

    /**
     * Default constructor needed to serialize/deserialize POJO object
     */
    public Point() {}

    public Point(Double[] dims) {
        this.dims = dims;
    }

    /**
     * Sum dimensions of nodes
     * @param other
     * @return this node with dimensions summed to the other
     */
    public Point addPoint(Point other) {
        for (int i = 0; i < this.dims.length; i++) {
            this.dims[i] = this.dims[i] + other.dims[i];
        }
        return this;
    }

    /**
     * Divide node dimensions for a scalar
     * @param val
     * @return this node modified
     */
    public Point divideScalar(long val) {
        for (int i = 0; i < this.dims.length; i++) {
            this.dims[i] = this.dims[i] / val;
        }
        return this;
    }

    /**
     * Calculate distance between nodes
     * @param other
     * @return calculated distance
     */
    public double distance(Point other) {
        Double sum = 0.0;
        for (int i = 0; i < this.dims.length; i++) {
            sum = sum + Math.pow(other.dims[i] - dims[i], 2);
        }
        return Math.sqrt(sum);
    }

    /**
     * Print dimensions of this point
     * @return node informations
     */
    @Override
    public String toString() {
        String s = "";
        for (int i = 0; i < dims.length; i++) {
            s = s + dims[i] + " ";
        }
        s = s.trim();
        return s;
    }
}
