package amu.saeed.kcminer.spark;

import amu.saeed.kcminer.graph.KCliqueState;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.common.hash.Hashing;
import org.apache.spark.Accumulator;
import org.apache.spark.Partitioner;
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
import java.io.Serializable;
import java.util.*;

public class Iterative3 {
    public static void main(String[] args) throws IOException {
        String appName = "Iterative KCMiner";
        SparkConf conf = new SparkConf().setAppName(appName);
        SparkConfigurator.config(conf);
        conf.registerKryoClasses(new Class[] {Tuple2.class, Integer.class, UType.class, KCliqueState.class});

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

        Partitioner partitioner = new Partitioner() {
            @Override public int numPartitions() {
                return params.numTasks;
            }

            @Override public int getPartition(Object o) {
                int var = (int) o;
                return var % numPartitions() >= 0 ? var % numPartitions() : var % numPartitions() + numPartitions();
            }
        };

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

        JavaPairRDD<Integer, UType> biggerNeighbors = edges.groupByKey().mapToPair(t -> {
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
            return UType.fromNeighbors(v, array).toTuple();
        }).partitionBy(partitioner).persist(StorageLevel.MEMORY_AND_DISK());

        JavaRDD<KCliqueState> states =
            biggerNeighbors.map(t -> new KCliqueState(t._1, t._2.neighbors)).filter(t -> t.extSize > 0);

        for (int iter = 2; iter < params.k; iter++) {
            final int iteration = iter;
            states = states.flatMapToPair(s -> {
                List<Tuple2<Integer, UType>> list = new ArrayList();
                for (int i = 0; i < s.extSize; i++)
                    list.add(UType.fromState(s.extension[i], s).toTuple());
                return list;
            }).union(biggerNeighbors).partitionBy(partitioner).mapPartitions(l -> {
                final HashMap<Integer, int[]> neighMap = new HashMap<>();
                final List<UType> stateList = new ArrayList<>();
                while (l.hasNext()) {
                    UType record = l.next()._2;
                    if (record.isNeighborList())
                        neighMap.put(record.w, record.neighbors);
                    else
                        stateList.add(record);
                }

                return Iterables.transform(stateList, s -> {
                    counts[iteration].add(1L);
                    return s.state.expand(s.w, neighMap.get(s.w));
                });
            }).filter(s -> s.extSize > 0);
        }


        counts[params.k].setValue(states.map(t -> (long) t.extSize).reduce((a, b) -> a + b));


        for (int i = 1; i < counts.length; i++)
            System.out.printf("Cliques of size %d => %,d\n", i, counts[i].value());

        sc.close();

        System.out.println("Took: " + stopwatch);
    }


    private static class UType {
        final int w;
        final int bucket;
        int[] neighbors = null;
        KCliqueState state = null;

        private UType(int w) {
            this.w = w;
            this.bucket = Hashing.murmur3_32().hashInt(w).asInt();
        }

        public static UType fromNeighbors(int w, int[] neighs) {
            UType res = new UType(w);
            res.neighbors = neighs;
            return res;
        }

        public static UType fromState(int w, KCliqueState state) {
            UType res = new UType(w);
            res.state = state;
            return res;
        }

        public Tuple2<Integer, UType> toTuple() {
            return new Tuple2<>(bucket, this);
        }

        public boolean isNeighborList() {
            return neighbors != null;
        }


    }


    private static class Params implements Serializable {
        @Option(name = "-local", usage = "run locally") boolean local = false;
        @Option(name = "-k", usage = "size of the cliques to mine.", required = true) int k;
        @Option(name = "-t", usage = "number of tasks to launch", required = true) int numTasks;
        @Option(name = "-p", usage = "number of graph partitions") int graphParts = 200;
        @Option(name = "-i", usage = "the input path", required = true) String inputPath;
    }
}
