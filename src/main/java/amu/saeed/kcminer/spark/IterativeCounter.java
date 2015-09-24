package amu.saeed.kcminer.spark;

import amu.saeed.kcminer.graph.KCliqueState;
import com.google.common.base.Stopwatch;
import com.google.common.hash.Hashing;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class IterativeCounter {
    public static void main(String[] args) throws IOException {
        String appName = "Iterative KCMiner";
        SparkConf conf = new SparkConf().setAppName(appName);
        SparkConfigurator.config(conf);
        conf.registerKryoClasses(new Class[] {Tuple2.class, Integer.class});

        for (String arg : args)
            if (arg.toLowerCase().equals("local")) conf.setMaster("local[8]");

        String input = args[0];
        int k = Integer.parseInt(args[1]);
        int numTasks = Integer.parseInt(args[2]);

        JavaSparkContext sc = new JavaSparkContext(conf);
        Stopwatch stopwatch = Stopwatch.createStarted();
        final Accumulator<Long>[] counts = new Accumulator[k + 1];
        for (int i = 0; i < counts.length; i++)
            counts[i] = LongAccumulator.create();

        JavaPairRDD<Integer, Integer> edges = sc.textFile(input).flatMapToPair(t -> {
            List<Tuple2<Integer, Integer>> list = new ArrayList();
            if (t.startsWith("#")) return list;
            String[] tokens = t.split("\\s+");
            int src = Integer.parseInt(tokens[0]);
            int dest = Integer.parseInt(tokens[1]);
            list.add(new Tuple2(src, dest));
            list.add(new Tuple2(dest, src));
            return list;
        }).repartition(numTasks);

        JavaPairRDD<Integer, int[]> biggerNeighbors = edges.groupByKey().mapToPair(t -> {
            counts[1].add((long) 1);
            int v = t._1;
            HashSet<Integer> bigs = new HashSet<>();
            for (Integer w : t._2)
                if (w > v) bigs.add(w);
            int[] array = new int[bigs.size()];
            int i = 0;
            for (Integer big : bigs)
                array[i++] = big;
            Arrays.sort(array);
            return new Tuple2<>(v, array);
        });

        biggerNeighbors = biggerNeighbors.persist(StorageLevel.MEMORY_AND_DISK());


        JavaRDD<KCliqueState> states = biggerNeighbors.map(t -> new KCliqueState(t._1, t._2)).repartition(numTasks);

        for (int iter = 2; iter < k; iter++) {
            final int iteration = iter;
            states = states.flatMapToPair(x -> {
                List<Tuple2<Integer, KCliqueState>> list = new ArrayList();
                for (int i = 0; i < x.extSize; i++)
                    list.add(new Tuple2<>(x.extension[i], x));
                return list;
            }).cogroup(biggerNeighbors, numTasks).flatMap(t -> {
                int w = t._1;
                int[] w_neighs = t._2._2.iterator().next();
                List<KCliqueState> list = new ArrayList<>();
                for (KCliqueState state : t._2._1)
                    list.add(state.expand(w, w_neighs));
                counts[iteration].add((long) list.size());
                return list;
            });
        }

        long kCliques = states.map(t -> (long) t.extSize).reduce((a, b) -> a + b);
        counts[k].setValue(kCliques);

        for (int i = 1; i < counts.length; i++)
            System.out.printf("Cliques of size %d => %,d\n", i, counts[i].value());

        sc.close();

        System.out.println("Took: " + stopwatch);

    }

    private int hash(int i, int mask) {
        return Hashing.murmur3_32().hashInt(i).asInt() & mask;
    }
}
