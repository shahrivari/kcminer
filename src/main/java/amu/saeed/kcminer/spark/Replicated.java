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
            conf.setMaster("local[8]");
        conf.set("spark.executor.memory", "4g");
        conf.set("spark.akka.frameSize", "128");
        conf.set("spark.storage.memoryFraction", "0.1");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        JavaSparkContext sc = new JavaSparkContext(conf);
        Accumulator<Long> cliqueCount = LongAccumulator.create();

        Graph graph = Graph.buildFromEdgeListFile(params.inputPath);
        final Broadcast<Graph> graphBroadcast = sc.broadcast(graph);
        ArrayList<Integer> vertices = new ArrayList(graph.vertices.length);
        for (int v : graph.vertices)
            if (graph.getBiggerNeighbors(v).length + 1 >= params.k)
                vertices.add(v);
        Collections.shuffle(vertices);

        Stopwatch stopwatch = Stopwatch.createStarted();

        JavaRDD<KCliqueState> twos = sc.parallelize(vertices).flatMap(
            t -> new KCliqueState(t, graphBroadcast.value().getBiggerNeighbors(t)).getChildren(graphBroadcast.value()))
            .repartition(params.graphParts);

        JavaRDD<KCliqueState> threes = twos.flatMap(t -> {
            ArrayList<KCliqueState> result = new ArrayList();
            for (KCliqueState threeState : t.getChildren(graphBroadcast.value()))
                if (threeState.clique.length + threeState.extSize >= params.k)
                    result.add(threeState);
            return result;
        }).repartition(params.numTasks);

        threes.map(t -> {
            cliqueCount.add(t.countKCliques(params.k, graphBroadcast.value()));
            return 1;
        }).count();

        int totalCores = sc.getConf().getInt("spark.cores.max", 0);
        System.out.printf("Graph:%s   Size:%d   Count:%,d   Cores:%d   Took:%s\n", params.inputPath, params.k,
            cliqueCount.value(), totalCores, stopwatch);
        sc.close();
    }

    private static class Params implements Serializable {
        @Option(name = "-local", usage = "run locally") boolean local = false;
        @Option(name = "-k", usage = "size of the cliques to mine.", required = true) int k;
        @Option(name = "-t", usage = "number of tasks to launch", required = true) int numTasks;
        @Option(name = "-p", usage = "number of graph partitions") int graphParts = 200;
        @Option(name = "-i", usage = "the input path", required = true) String inputPath;
    }

}
