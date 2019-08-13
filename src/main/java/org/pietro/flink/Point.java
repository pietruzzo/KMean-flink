package org.pietro.flink;

import java.io.Serializable;

/**
 * Multidimensional Point
 * POJO object
 */
public class Point implements Serializable {

    public double p1, p2;

    /**
     * Default constructor needed to serialize/deserialize POJO object
     */
    public Point() {}

    public Point(double p1, double p2) {
        this.p1 = p1;
        this.p2 = p2;
    }

    /**
     * Sum dimensions of nodes
     * @param other
     * @return this node with dimensions summed to the other
     */
    public Point addPoint(Point other) {
        this.p1 = p1 + other.p1;
        this.p2 = p2 + other.p2;
        return this;
    }

    /**
     * Divide node dimensions for a scalar
     * @param val
     * @return this node modified
     */
    public Point divideScalar(long val) {
        this.p1 = p1 / val;
        this.p2 = p2 / val;
        return this;
    }

    /**
     * Calculate distance between nodes
     * @param other
     * @return calculated distance
     */
    public double distance(Point other) {
        double sum = Math.pow(other.p1 - p1, 2);;
            sum = sum + Math.pow(other.p2 - p2, 2);
        return sum;
    }

    /**
     * Print dimensions of this point
     * @return node informations
     */
    @Override
    public String toString() {
        return p1 + " " + p2;
    }
}
