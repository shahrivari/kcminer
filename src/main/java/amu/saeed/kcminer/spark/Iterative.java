package amu.saeed.kcminer.spark;

import amu.saeed.kcminer.graph.KCliqueState;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

public class Iterative {
    public static void main(String[] args) throws IOException {
        String appName = "Iterative KCMiner";
        SparkConf conf = new SparkConf().setAppName(appName);
        SparkConfigurator.config(conf);
        conf.registerKryoClasses(new Class[] {Tuple2.class, Integer.class});

        Params params = new Params();
        try {
            new CmdLineParser(params).parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            new CmdLineParser(params).printUsage(System.err);
            return;
        }

        if (params.local)
            conf.setMaster("local[8]");


        JavaSparkContext sc = new JavaSparkContext(conf);
        Stopwatch stopwatch = Stopwatch.createStarted();
        final Accumulator<Long>[] counts = new Accumulator[params.k + 1];
        for (int i = 0; i < counts.length; i++)
            counts[i] = LongAccumulator.create();

        JavaPairRDD<Integer, Integer> edges = sc.textFile(params.inputPath).flatMapToPair(t -> {
            List<Tuple2<Integer, Integer>> list = new ArrayList();
            if (t.startsWith("#"))
                return list;
            String[] tokens = t.split("\\s+");
            int src = Integer.parseInt(tokens[0]);
            int dest = Integer.parseInt(tokens[1]);
            list.add(new Tuple2(src, dest));
            list.add(new Tuple2(dest, src));
            return list;
        }).repartition(params.graphParts);

        JavaPairRDD<Integer, int[]> biggerNeighbors = edges.groupByKey().mapToPair(t -> {
            counts[1].add((long) 1);
            int v = t._1;
            HashSet<Integer> bigs = new HashSet<>();
            for (Integer w : t._2)
                if (w > v)
                    bigs.add(w);
            int[] array = new int[bigs.size()];
            int i = 0;
            for (Integer big : bigs)
                array[i++] = big;
            Arrays.sort(array);
            return new Tuple2<>(v, array);
        });

        biggerNeighbors = biggerNeighbors.repartition(params.graphParts).persist(StorageLevel.MEMORY_AND_DISK());



        JavaRDD<KCliqueState> states =
            biggerNeighbors.map(t -> new KCliqueState(t._1, t._2)).repartition(params.numTasks);

        for (int iter = 2; iter < params.k; iter++) {
            final int iteration = iter;
            states = states.flatMapToPair(x -> {
                List<Tuple2<Integer, KCliqueState>> list = new ArrayList();
                for (int i = 0; i < x.extSize; i++)
                    list.add(new Tuple2<>(x.extension[i], x));
                return list;
            }).cogroup(biggerNeighbors, params.numTasks).flatMap(t -> {
                int w = t._1;
                Iterator<int[]> w_iter = t._2._2.iterator();
                int[] w_neighs = w_iter.next();
                if (w_iter.hasNext())
                    throw new IllegalStateException("This must not happen!");
                return Iterables.transform(t._2._1, s -> {
                    counts[iteration].add(1L);
                    return s.expand(w, w_neighs);
                });
            });

            //            join(biggerNeighbors, params.numTasks).flatMap(t -> {
            //                int w = t._1;
            //                int[] w_neighs = t._2._2;
            //                List<KCliqueState> list = new ArrayList<>();
            //                list.add(t._2._1.expand(w, w_neighs));
            //                counts[iteration].add((long) list.size());
            //                return list;
            //            });
        }

        final int k = params.k;
        states.map(t -> {
            counts[k].add((long) t.extSize);
            return t.extSize;
        }).count();


        for (int i = 1; i < counts.length; i++)
            System.out.printf("Cliques of size %d => %,d\n", i, counts[i].value());

        sc.close();

        System.out.println("Took: " + stopwatch);
    }


    private static class Params {
        @Option(name = "-local", usage = "run locally") boolean local = false;
        @Option(name = "-k", usage = "size of the cliques to mine.", required = true) int k;
        @Option(name = "-t", usage = "number of tasks to launch", required = true) int numTasks;
        @Option(name = "-p", usage = "number of graph partitions") int graphParts = 200;
        @Option(name = "-i", usage = "the input path", required = true) String inputPath;
    }
}
