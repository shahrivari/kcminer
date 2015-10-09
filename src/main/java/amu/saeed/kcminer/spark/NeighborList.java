package amu.saeed.kcminer.spark;

import amu.saeed.kcminer.graph.KCliqueState;
import com.google.common.base.Stopwatch;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class NeighborList {
    public static void main(String[] args) throws IOException {
        String appName = "Neighbor List";
        SparkConf conf = new SparkConf().setAppName(appName);
        SparkConfigurator.config(conf);
        conf.registerKryoClasses(new Class[] {int[].class, Tuple2.class, Integer.class, KCliqueState.class});

        Params params = new Params();
        try {
            new CmdLineParser(params).parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            new CmdLineParser(params).printUsage(System.err);
            return;
        }


        JavaSparkContext sc = new JavaSparkContext(conf);
        Stopwatch stopwatch = Stopwatch.createStarted();


        // We have the edges.
        JavaPairRDD<Integer, Integer> edges = sc.textFile(params.inputPath).flatMapToPair(t -> {
            List<Tuple2<Integer, Integer>> list = new ArrayList();
            if (t.startsWith("#"))
                return list;
            String[] tokens = t.split("\\s+");
            int src = Integer.parseInt(tokens[0]);
            int dest = Integer.parseInt(tokens[1]);
            // put both sides to reach an undirected graph
            list.add(new Tuple2(src, dest));
            list.add(new Tuple2(dest, src));
            return list;
        }).repartition(params.graphParts);

        edges.groupByKey(params.graphParts).map(t -> {
            int v = t._1;
            HashSet<Integer> neighbors = new HashSet<>();
            for (Integer w : t._2)
                neighbors.add(w);
            int[] array = new int[neighbors.size()];
            int i = 0;
            for (Integer vv : neighbors)
                array[i++] = vv;
            Arrays.sort(array);
            StringBuilder builder = new StringBuilder();
            builder.append(v);
            for (int j = 0; j < array.length; j++)
                builder.append(' ').append(array[j]);

            return builder.toString();
        }).saveAsTextFile(params.outputPath);

        System.out.println("Took: " + stopwatch);

    }



    private static class Params implements Serializable {
        @Option(name = "-p", usage = "number of graph partitions") int graphParts = 200;
        @Option(name = "-i", usage = "the input path", required = true) String inputPath;
        @Option(name = "-o", usage = "the output path") String outputPath;
    }
}
