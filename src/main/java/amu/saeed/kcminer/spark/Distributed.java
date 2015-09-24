package amu.saeed.kcminer.spark;

import amu.saeed.kcminer.graph.Graph;
import amu.saeed.kcminer.graph.KCliqueState;
import com.google.common.base.Stopwatch;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by Saeed on 8/21/2015.
 */
public class Distributed {
    public static void main(String[] args) throws IOException {
        String appName = "Replicated KCMiner";
        SparkConf conf = new SparkConf().setAppName(appName);
        conf.set("spark.executor.memory", "16g");
        conf.set("spark.akka.frameSize", "128");
        conf.set("spark.executor.extraJavaOptions",
            "-XX:+UseParallelGC -XX:+UseParallelOldGC " + "-XX:ParallelGCThreads=3 -XX:MaxGCPauseMillis=100");
        conf.set("spark.storage.memoryFraction", "0.33");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");


        JavaSparkContext sc = new JavaSparkContext(conf);
        Graph graph = Graph.buildFromEdgeListFile(args[0]);
        final Broadcast<Graph> graphBroadcast = sc.broadcast(graph);
        ArrayList<Integer> vertices = new ArrayList(graph.vertices.length);
        for (int v : graph.vertices)
            vertices.add(v);

        Collections.shuffle(vertices);

        Stopwatch stopwatch = Stopwatch.createStarted();

        int k = Integer.parseInt(args[1]);
        int numTasks = Integer.parseInt(args[2]);
        JavaRDD<Integer> verticesRDD = sc.parallelize(vertices).repartition(numTasks);

        Accumulator<Long> cliqueCount = LongAccumulator.create();

        JavaRDD<KCliqueState> twos = verticesRDD.flatMap(t -> {
            Graph localG = graphBroadcast.value();
            KCliqueState oneClique = new KCliqueState(t, localG.getBiggerNeighbors(t));
            return oneClique.getChildren(localG);
        }).repartition(numTasks);


        JavaRDD<KCliqueState> threes = twos.flatMap(t -> {
            ArrayList<KCliqueState> result = new ArrayList();
            Graph localG = graphBroadcast.value();
            List<KCliqueState> threeStates = t.getChildren(localG);
            for (KCliqueState threeState : threeStates)
                if (threeState.clique.length + threeState.extSize >= k)
                    result.add(threeState);
            return result;
        }).repartition(numTasks);

        threes.map(t -> {
            Graph localG = graphBroadcast.value();
            cliqueCount.add(t.countKCliques(k, localG));
            return null;
        }).count();

        sc.close();

        System.out.println("Took: " + stopwatch);
        System.out.printf("Total cliques of size %d => %,d \n", k, cliqueCount.value());


    }
}
