package org.pietro.flink;

/**
 * Centroid: a Point with an identifier
 */
public class Centroid extends Point {

    public int id;

    /**
     * Default constructor needed to serialize/deserialize POJO object
     */
    public Centroid() {}

    public Centroid(int id, Double[] dims) {
        super(dims);
        this.id = id;
    }

    public Centroid(int id, Point p) {
        super(p.dims);
        this.id = id;
    }

    @Override
    public String toString() {
        return id + " " + super.toString();
    }
}