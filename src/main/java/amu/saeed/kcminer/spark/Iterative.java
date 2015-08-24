package amu.saeed.kcminer.spark;

import amu.saeed.kcminer.graph.Graph;
import amu.saeed.kcminer.graph.KCliqueState;
import com.google.common.base.Stopwatch;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Saeed on 8/21/2015.
 */
public class Iterative {
    public static void main(String[] args) throws IOException {
        String appName = "Replicated KCMiner";
        SparkConf conf = new SparkConf().setAppName(appName).setMaster("local[6]");
        //conf.set("spark.executor.memory", "16g");
        conf.set("spark.akka.frameSize", "128");
        conf.set("spark.executor.extraJavaOptions",
            "-XX:+UseParallelGC -XX:+UseParallelOldGC " + "-XX:ParallelGCThreads=3 -XX:MaxGCPauseMillis=100");
        conf.set("spark.storage.memoryFraction", "0.3");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        int k = Integer.parseInt(args[1]);
        int numTasks = Integer.parseInt(args[2]);

        final Graph graph = Graph.buildFromEdgeListFile(args[0]);

        JavaSparkContext sc = new JavaSparkContext(conf);
        Stopwatch stopwatch = Stopwatch.createStarted();

        JavaPairRDD<Integer, Integer> edges = sc.textFile(args[0]).flatMapToPair(t -> {
            List<Tuple2<Integer, Integer>> list = new ArrayList();
            if (t.startsWith("#"))
                return list;
            String[] tokens = t.split("\\s+");
            int src = Integer.parseInt(tokens[0]);
            int dest = Integer.parseInt(tokens[1]);
            list.add(new Tuple2(src, dest));
            list.add(new Tuple2(dest, src));
            return list;
        });

        JavaPairRDD<Integer, int[]> biggerNeighbors = edges.groupByKey().mapToPair(t -> {
            int v = t._1;
            List<Integer> bigs = new ArrayList();
            for (Integer w : t._2)
                if (w > v)
                    bigs.add(w);
            int[] array = new int[bigs.size()];
            for (int i = 0; i < bigs.size(); i++)
                array[i] = bigs.get(i);
            Arrays.sort(array);
            return new Tuple2<Integer, int[]>(v, array);
        }).repartition(numTasks);


        long actual = biggerNeighbors.count();

        JavaRDD<Integer> mis =
            biggerNeighbors.map(t -> Arrays.equals(graph.getBiggerNeighbors(t._1), t._2) == true ? 1 : 0);

        long msc = mis.count();

        JavaRDD<KCliqueState> states = biggerNeighbors.map(t -> new KCliqueState(t._1, t._2)).repartition(numTasks);

        long count = states.count();
        System.out.printf("Total cliques of size %d => %,d \n", k, count);


        JavaPairRDD<Integer, KCliqueState> readyToExpand = states.flatMapToPair(t -> {
            List<Tuple2<Integer, KCliqueState>> list = new ArrayList();
            for (int i = 0; i < t.extSize; i++)
                list.add(new Tuple2(t.extension[i], t));
            return list;
        });

        JavaPairRDD<Integer, Tuple2<KCliqueState, int[]>> joined = readyToExpand.join(biggerNeighbors, numTasks);
        states = joined.map(t -> t._2._1.expand(t._1, t._2._2)).filter(t -> t != null);

        count = states.count();
        System.out.printf("Total cliques of size %d => %,d \n", k, count);



        //                for(int iter=2;iter<k;iter++){
        //            JavaPairRDD<Integer, KCliqueState> readyToExpand = states.flatMapToPair(t -> {
        //                List<Tuple2<Integer, KCliqueState>> list = new ArrayList();
        //                for (int i = 0; i < t.extSize; i++)
        //                    list.add(new Tuple2(t.extension[i], t));
        //                return list;
        //            });
        //            JavaPairRDD<Integer, Tuple2<KCliqueState, int[]>> joined = readyToExpand.join(biggerNeighbors,
        // numTasks);
        //            states=joined.map(t -> t._2._1.expand(t._1,t._2._2)).filter(t->t!=null);
        //        }


        sc.close();

        System.out.println("Took: " + stopwatch);

    }
}
