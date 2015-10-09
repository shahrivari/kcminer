package amu.saeed.kcminer.spark;

import amu.saeed.kcminer.graph.Graph;
import amu.saeed.kcminer.graph.KCliqueState;
import com.google.common.base.Stopwatch;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by Saeed on 8/21/2015.
 */
public class Replicated {
    public static void main(String[] args) throws IOException {
        Params params = new Params();
        try {
            new CmdLineParser(params).parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            new CmdLineParser(params).printUsage(System.err);
            return;
        }


        String appName = "Replicated KCMiner";
        SparkConf conf = new SparkConf().setAppName(appName);
        if (params.local)
            conf.setMaster("local[6]");
        conf.set("spark.executor.memory", "16g");
        conf.set("spark.akka.frameSize", "128");
        conf.set("spark.executor.extraJavaOptions",
            "-XX:+UseParallelGC -XX:+UseParallelOldGC " + "-XX:ParallelGCThreads=3 -XX:MaxGCPauseMillis=100");
        conf.set("spark.storage.memoryFraction", "0.1");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        JavaSparkContext sc = new JavaSparkContext(conf);


        Graph graph = Graph.buildFromEdgeListFile(params.inputPath);
        final Broadcast<Graph> graphBroadcast = sc.broadcast(graph);
        ArrayList<Integer> vertices = new ArrayList(graph.vertices.length);
        for (int v : graph.vertices)
            vertices.add(v);

        Collections.shuffle(vertices);

        Stopwatch stopwatch = Stopwatch.createStarted();

        JavaRDD<Integer> verticesRDD = sc.parallelize(vertices).repartition(params.graphParts);

        Accumulator<Long> cliqueCount = LongAccumulator.create();

        JavaRDD<KCliqueState> twos = verticesRDD.flatMap(t -> {
            Graph localG = graphBroadcast.value();
            KCliqueState oneClique = new KCliqueState(t, localG.getBiggerNeighbors(t));
            return oneClique.getChildren(localG);
        }).repartition(params.graphParts);


        JavaRDD<KCliqueState> threes = twos.flatMap(t -> {
            ArrayList<KCliqueState> result = new ArrayList();
            Graph localG = graphBroadcast.value();
            List<KCliqueState> threeStates = t.getChildren(localG);
            for (KCliqueState threeState : threeStates)
                if (threeState.clique.length + threeState.extSize >= params.k)
                    result.add(threeState);
            return result;
        }).repartition(params.numTasks);

        threes.map(t -> {
            Graph localG = graphBroadcast.value();
            cliqueCount.add(t.countKCliques(params.k, localG));
            return null;
        }).count();

        sc.close();

        System.out.println("Took: " + stopwatch);
        System.out.printf("Total cliques of size %d => %,d \n", params.k, cliqueCount.value());
    }

    private static class Params implements Serializable {
        @Option(name = "-local", usage = "run locally") boolean local = false;
        @Option(name = "-k", usage = "size of the cliques to mine.", required = true) int k;
        @Option(name = "-t", usage = "number of tasks to launch", required = true) int numTasks;
        @Option(name = "-p", usage = "number of graph partitions") int graphParts = 200;
        @Option(name = "-i", usage = "the input path", required = true) String inputPath;
    }

}
