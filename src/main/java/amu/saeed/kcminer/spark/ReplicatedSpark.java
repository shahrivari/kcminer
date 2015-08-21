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

/**
 * Created by Saeed on 8/21/2015.
 */
public class ReplicatedSpark {
    public static void main(String[] args) throws IOException {
        String appName = "Replicated KCMiner";
        SparkConf conf = new SparkConf().setAppName(appName);
        conf.set("spark.executor.memory", "16g");
        conf.set("spark.akka.frameSize", "128");
        conf.set("spark.executor.extraJavaOptions",
            "-XX:+UseParallelGC -XX:+UseParallelOldGC " + "-XX:ParallelGCThreads=3 -XX:MaxGCPauseMillis=100");
        conf.set("spark.storage.memoryFraction", "0.1");
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
        JavaRDD<Integer> verticesRDD = sc.parallelize(vertices).repartition(Integer.parseInt(args[2]));

        Accumulator<Long> cliqueCount = LongAccumolator.create();

        //        verticesRDD.mapPartitions(t -> {
        //            Graph localG = graphBroadcast.value();
        //            long localcount = 0;
        //            while (t.hasNext()) {
        //                int v=t.next();
        //                localcount += new KCliqueState(v, graph.getBiggerNeighbors(v), graph.getSmallerNeighbors(v))
        //                    .countKCliques(k, graph);
        //            }
        //            cliqueCount.add(localcount);
        //            ArrayList<Long> res = new ArrayList<Long>();
        //            res.add(localcount);
        //            return res;
        //        }).count();

        JavaRDD<KCliqueState> twos = verticesRDD.map(t -> {
            Graph localG = graphBroadcast.value();
            return new KCliqueState(t, localG.getBiggerNeighbors(t), localG.getSmallerNeighbors(t));
        });

        twos.map(t -> {
            Graph localG = graphBroadcast.value();
            cliqueCount.add(t.countKCliques(k, localG));
            return null;
        }).count();

        sc.close();

        System.out.println("Took: " + stopwatch);
        System.out.printf("Total cliques of size %d => %,d \n", k, cliqueCount.value());


    }
}
