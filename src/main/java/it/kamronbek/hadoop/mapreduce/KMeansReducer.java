package it.kamronbek.hadoop.mapreduce;

import java.io.IOException;

import it.kamronbek.hadoop.model.Point;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansReducer extends Reducer<IntWritable, Point, Text, Text> {

    private final Text centroidId = new Text();
    private final Text centroidValue = new Text();

    @Override
    public void reduce(IntWritable centroid, Iterable<Point> partialSums, Context context)
        throws IOException, InterruptedException {

        // Sum the partial sums
        Point sum = Point.copy(partialSums.iterator().next());
        while (partialSums.iterator().hasNext()) {
            sum.sum(partialSums.iterator().next());
        }
        // Calculate the new centroid
        sum.average();

        centroidId.set(centroid.toString());
        centroidValue.set(sum.toString());
        context.write(centroidId, centroidValue);
    }
}