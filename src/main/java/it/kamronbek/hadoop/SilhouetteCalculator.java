package it.kamronbek.hadoop;

import it.kamronbek.hadoop.model.Cluster;
import it.kamronbek.hadoop.model.Point;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SilhouetteCalculator {

    public static double calculate(List<Point> points, Point[] centroids) {
        // Insert cluster centers
        List<Cluster> clusters = Arrays.stream(centroids)
                .map(Cluster::new)
                .collect(Collectors.toList());

        // Insert the points to their respective clusters
        for (Point point : points) {
            Cluster cluster = clusters.get(0);
            for (int i = 1; i < clusters.size(); i++) {
                Cluster temp = clusters.get(i);
                if (temp.centroid.distance(point) < cluster.centroid.distance(point))
                    cluster = temp;
            }
            cluster.points.add(point);
        }

        return clusters.parallelStream()
                // Take the average silhouette score of all clusters
                .mapToDouble(cluster -> cluster.points
                        // Take the average of silhouette score of all points for every cluster
                        .stream().mapToDouble(point -> {
                            // Calculate the silhouette for the point inside its cluster
                            double silhouetteInItsCluster = cluster.points
                                    .stream().mapToDouble(p -> p.distance(point)).sum() / (cluster.points.size() - 1);
                            // Calculate the silhouette for the point versus nearest cluster
                            double minSilhouetteInExternalClusters = clusters.
                                    stream()
                                    .filter(c -> !c.equals(cluster))
                                    .mapToDouble(
                                            c -> c.points.stream().mapToDouble(p -> p.distance(point)).sum() / c.points.size()
                                    ).min().getAsDouble();
                            // return (b - a) / Max(a, b)
                            return (minSilhouetteInExternalClusters - silhouetteInItsCluster)
                                    / Math.max(minSilhouetteInExternalClusters, silhouetteInItsCluster);
                        }).sum() / cluster.points.size())
                .sum() / clusters.size();
    }
}
