package amu.saeed.kcminer.smp;

import com.google.common.base.Stopwatch;

import java.io.IOException;

/**
 * Created by Saeed on 8/15/2015.
 */
public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        int size = 7;
        int threads = 8;
        String graph_path = "X:\\networks\\kcminer\\epin.txt";
        Graph graph;
        long count;
        Stopwatch stopwatch = Stopwatch.createUnstarted();

        stopwatch.reset().start();
        graph = Graph.buildFromEdgeListFile(graph_path);
        System.out.println(graph.getInfo());
        System.out.println("Took:" + stopwatch);
        stopwatch.reset().start();
        count = CliqueState.parallelEnumerate(new RawCliqueStateManager(), graph, size, threads, null);
        System.out.printf("%,d\n", count);
        System.out.println("Took:" + stopwatch);
        System.out.println("=========================");


        stopwatch.reset().start();
        graph = PrunedGraph.buildFromEdgeListFile(graph_path);
        System.out.println(graph.getInfo());
        System.out.println("Took:" + stopwatch);
        stopwatch.reset().start();
        count = CliqueState.parallelEnumerate(new PrunedCliqueStateManager(), graph, size, threads, null);
        System.out.printf("%,d\n", count);
        System.out.println("Took:" + stopwatch);
        System.out.println("=========================");

        stopwatch.reset().start();
        graph = PrunedGraph.buildFromEdgeListFile(graph_path);
        System.out.println(graph.getInfo());
        System.out.println("Took:" + stopwatch);
        stopwatch.reset().start();
        count = CliqueState.parallelCountRecursive(new PrunedCliqueStateManager(), graph, size, threads);
        System.out.printf("%,d\n", count);
        System.out.println("Took:" + stopwatch);


    }

}
