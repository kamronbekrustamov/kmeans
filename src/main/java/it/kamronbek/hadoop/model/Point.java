package it.kamronbek.hadoop.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import org.apache.hadoop.io.Writable;

public class Point implements Writable {
    
    private double[] components = null;
    private int dimension;
    private int numPoints; // For partial sums

    public Point() {
        this.dimension = 0;
    }
    
    public Point(final double[] c) {
        this.set(c);
    }

    public Point(final String[] s) { 
        this.set(s);
    }

    public static Point copy(final Point p) {
        Point ret = new Point(p.components);
        ret.numPoints = p.numPoints;
        return ret;
    }
    
    public void set(final double[] c) {
        this.components = c;
        this.dimension = c.length;
        this.numPoints = 1;
    }

    public void set(final String[] s) {
        this.components = new double[s.length];
        this.dimension = s.length;
        this.numPoints = 1;
        for (int i = 0; i < s.length; i++) {
            this.components[i] = Double.parseDouble(s[i]);
        }
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        this.dimension = in.readInt();
        this.numPoints = in.readInt();
        this.components = new double[this.dimension];

        for(int i = 0; i < this.dimension; i++) {
            this.components[i] = in.readDouble();
        }
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeInt(this.dimension);
        out.writeInt(this.numPoints);

        for(int i = 0; i < this.dimension; i++) {
            out.writeDouble(this.components[i]);
        }
    }

    @Override
    public String toString() {
        StringBuilder point = new StringBuilder();
        for (int i = 0; i < this.dimension; i++) {
            point.append(this.components[i]);
            if(i != dimension - 1) {
                point.append(",");
            }   
        }
        return point.toString();
    }

    public void sum(Point p) {
        for (int i = 0; i < this.dimension; i++) {
            this.components[i] += p.components[i];
        }
        this.numPoints += p.numPoints;
    }

    public double distance(Point p) {
        return distance(p, 2);
    }
    public double distance(Point p, int h){
        if (h < 0) {
            // Consider only metric distances
            h = 2;   
        }
        
        if (h == 0) {
            // Chebyshev
            double max = -1f;
            double diff;
            for (int i = 0; i < this.dimension; i++) {
                diff = Math.abs(this.components[i] - p.components[i]);
                if (diff > max) {
                    max = diff;
                }                       
            }
            return max;

        } else {
            // Manhattan, Euclidean, Minkowsky
            double dist = 0.0;
            for (int i = 0; i < this.dimension; i++) {
                dist += Math.pow(Math.abs(this.components[i] - p.components[i]), h);
            }
            dist = Math.round(Math.pow(dist, 1.0 / h) * 100000) / 100000.0;
            return dist;
        }
    }
    
    public void average() {
        for (int i = 0; i < this.dimension; i++) {
            double temp = this.components[i] / this.numPoints;
            this.components[i] = Math.round(temp * 100000) / 100000.0;
        }
        this.numPoints = 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Point point = (Point) o;
        return dimension == point.dimension && Arrays.equals(components, point.components);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(dimension);
        result = 31 * result + Arrays.hashCode(components);
        return result;
    }
}