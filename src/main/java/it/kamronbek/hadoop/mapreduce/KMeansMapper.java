package it.kamronbek.hadoop.mapreduce;

import java.io.IOException;

import it.kamronbek.hadoop.model.Point;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Point> {

    private Point[] centroids;
    private int p;
    private final Point point = new Point();
    private final IntWritable centroid = new IntWritable();

    @Override
    public void setup(Context context) {
        int k = Integer.parseInt(context.getConfiguration().get("k"));
        this.p = Integer.parseInt(context.getConfiguration().get("distance"));

        this.centroids = new Point[k];
        for(int i = 0; i < k; i++)
            this.centroids[i] = new Point(context.getConfiguration().getStrings("centroid." + i));
    }

    @Override
    public void map(LongWritable key, Text value, Context context) 
     throws IOException, InterruptedException {
        
        // Construct the point
        String[] pointString = value.toString().split(",");
        point.set(pointString);

        // Initialize variables
        double minDist = Float.POSITIVE_INFINITY;
        double distance;
        int nearest = -1;

        // Find the closest centroid
        for (int i = 0; i < centroids.length; i++) {
            distance = point.distance(centroids[i], p);
            if(distance < minDist) {
                nearest = i;
                minDist = distance;
            }
        }

        centroid.set(nearest);
        context.write(centroid, point);
    }
}

