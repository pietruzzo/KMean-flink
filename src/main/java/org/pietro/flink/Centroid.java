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

    public Centroid(int id, double p1, double p2) {
        super(p1, p2);
        this.id = id;
    }

    public Centroid(int id, Point p) {
        super(p.p1, p.p2);
        this.id = id;
    }

    @Override
    public String toString() {
        return id + " " + super.toString();
    }
}