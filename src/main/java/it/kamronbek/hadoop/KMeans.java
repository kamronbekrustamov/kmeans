package it.kamronbek.hadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import it.kamronbek.hadoop.mapreduce.KMeansCombiner;
import it.kamronbek.hadoop.mapreduce.KMeansMapper;
import it.kamronbek.hadoop.mapreduce.KMeansReducer;
import it.kamronbek.hadoop.model.Point;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class KMeans {

    public static void main(String[] args) throws Exception {
        long start;
        long end;
        long startIC;
        long endIC;

        start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        conf.addResource(new Path("config.xml")); // Configuration file for the parameters

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: <input> <output>");
            System.exit(1);
        }

        // Parameters setting
        final String INPUT = otherArgs[0];
        final String OUTPUT = otherArgs[1] + "/temp";
        final int DATASET_SIZE = conf.getInt("dataset", 10);
        final int DISTANCE = conf.getInt("distance", 2);
        final int K = conf.getInt("k", 3);
        final double THRESHOLD = conf.getDouble("threshold", 0.0001);
        final int MAX_ITERATIONS = conf.getInt("max.iteration", 30);

        Point[] oldCentroids = new Point[K];
        Point[] newCentroids;

        // Initialize centroids
        startIC = System.currentTimeMillis();
        newCentroids = centroidsInit(conf, INPUT, K, DATASET_SIZE);
        endIC = System.currentTimeMillis();
        for(int i = 0; i < K; i++) {
            conf.set("centroid." + i, newCentroids[i].toString());
        }

        // MapReduce workflow
        boolean stop = false;
        boolean succeded;
        int i = 0;
        while(!stop) {
            i++;

            //Job configuration
            Job iteration = Job.getInstance(conf, "iter_" + i);
            iteration.setJarByClass(KMeans.class);
            iteration.setMapperClass(KMeansMapper.class);
            iteration.setCombinerClass(KMeansCombiner.class);
            iteration.setReducerClass(KMeansReducer.class);
            iteration.setNumReduceTasks(K); // one task each centroid
            iteration.setOutputKeyClass(IntWritable.class);
            iteration.setOutputValueClass(Point.class);
            FileInputFormat.addInputPath(iteration, new Path(INPUT));
            FileOutputFormat.setOutputPath(iteration, new Path(OUTPUT));
            iteration.setInputFormatClass(TextInputFormat.class);
            iteration.setOutputFormatClass(TextOutputFormat.class);

            succeded = iteration.waitForCompletion(true);

            //If the job fails the application will be closed.
            if(!succeded) {
                System.err.println("Iteration" + i + "failed.");
                System.exit(1);
            }

            // Save old centroids and read new centroids
            for(int id = 0; id < K; id++) {
                oldCentroids[id] = Point.copy(newCentroids[id]);
            }
            newCentroids = readCentroids(conf, K, OUTPUT);

            // Check if centroids are changed
            stop = stoppingCriterion(oldCentroids, newCentroids, DISTANCE, THRESHOLD);

            if(stop || i == (MAX_ITERATIONS -1)) {
                double silhouette = SilhouetteCalculator.calculate(getAllPoints(conf, INPUT), newCentroids);
                finish(conf, newCentroids, otherArgs[1], silhouette);
            } else {
                // Set the new centroids in the configuration
                for(int d = 0; d < K; d++) {
                    conf.unset("centroid." + d);
                    conf.set("centroid." + d, newCentroids[d].toString());
                }
            }
        }

        end = System.currentTimeMillis();

        end -= start;
        endIC -= startIC;

        System.out.println("execution time: " + end + " ms");
        System.out.println("init centroid execution: " + endIC + " ms");
        System.out.println("n_iter: " + i);

        System.exit(0);
    }


    private static boolean stoppingCriterion(Point[] oldCentroids, Point[] newCentroids, int distance, double threshold) {
        for(int i = 0; i < oldCentroids.length; i++) {
            boolean check = oldCentroids[i].distance(newCentroids[i], distance) <= threshold;
            if(!check) {
                return false;
            }
        }
        return true;
    }

    private static Point[] centroidsInit(Configuration conf, String pathString, int k, int dataSetSize) throws IOException {
    	Point[] points = new Point[k];
        // Create a sorted list of positions without duplicates
        // Positions are the line index of the random selected centroids
        List<Integer> positions = new ArrayList<>();
        Random random = new Random();
        int pos;
        while(positions.size() < k) {
            pos = random.nextInt(dataSetSize);
            if(!positions.contains(pos)) {
                positions.add(pos);
            }
        }
        Collections.sort(positions);
        
        // File reading utils
        Path path = new Path(pathString);
    	FileSystem hdfs = FileSystem.get(conf);
    	FSDataInputStream in = hdfs.open(path);
        try(BufferedReader br = new BufferedReader(new InputStreamReader(in))) {

            // Get centroids from the file
            int row = 0;
            int i = 0;
            int position;
            while (i < positions.size()) {
                position = positions.get(i);
                String point = br.readLine();
                if (row == position) {
                    points[i] = new Point(point.split(","));
                    i++;
                }
                row++;
            }
        }
    	return points;
    }

    private static Point[] readCentroids(Configuration conf, int k, String pathString)
      throws IOException {
        Point[] points = new Point[k];
        FileSystem hdfs = FileSystem.get(conf);
        FileStatus[] status = hdfs.listStatus(new Path(pathString));

        for (FileStatus fileStatus : status) {
            // Read the centroids from the hdfs
            if (!fileStatus.getPath().toString().endsWith("_SUCCESS")) {
                try(BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(fileStatus.getPath())))) {
                    String[] keyValueSplit = br.readLine().split("\t"); // Split line in K,V
                    int centroidId = Integer.parseInt(keyValueSplit[0]);
                    String[] point = keyValueSplit[1].split(",");
                    points[centroidId] = new Point(point);
                }
            }
        }
        // Delete temp directory
        hdfs.delete(new Path(pathString), true); 

    	return points;
    }

    private static void finish(Configuration conf, Point[] centroids, String output, double silhouette) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        FSDataOutputStream dos = hdfs.create(new Path(output + "/centroids.txt"), true);
        try(BufferedWriter br = new BufferedWriter(new OutputStreamWriter(dos))) {

            // Write the result in a unique file
            for (Point centroid : centroids) {
                br.write(centroid.toString());
                br.newLine();
            }
            br.write("Silhouette: " + silhouette);
        }
        hdfs.close();
    }


    private static List<Point> getAllPoints(Configuration conf, String pathString) throws IOException {

        try(BufferedReader br = new BufferedReader(new InputStreamReader(FileSystem.get(conf).open(new Path(pathString))))) {
            return br.lines().collect(Collectors.toList())
                    .stream().map(line -> new Point(line.split(","))).collect(Collectors.toList());
        }
    }
}