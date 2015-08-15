package amu.saeed.kcminer.smp;

import com.google.common.base.Stopwatch;

import java.io.IOException;

/**
 * Created by Saeed on 8/15/2015.
 */
public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        Graph graph = Graph.buildFromEdgeListFile("X:\\networks\\kcminer\\epin.txt");
        System.out.println(graph.getInfo());
        System.out.println("Took:" + stopwatch);
        stopwatch.reset().start();

        int size = 7;
        int threads = 8;
        long count = KlikState.parallelEnumerate(graph, size, size, threads, null);
        System.out.printf("%,d\n", count);
        System.out.println("Took:" + stopwatch);

    }

}
