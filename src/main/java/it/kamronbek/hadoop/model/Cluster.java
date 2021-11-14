package it.kamronbek.hadoop.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Cluster {

    public Point centroid;
    public List<Point> points;

    public Cluster(Point centroid) {
        this.centroid = centroid;
        this.points = new ArrayList<>();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Cluster cluster = (Cluster) o;
        return centroid.equals(cluster.centroid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(centroid);
    }
}