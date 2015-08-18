package amu.saeed.kcminer.smp;

import amu.saeed.kcminer.graph.Graph;
import amu.saeed.kcminer.old.OldGraph;
import com.google.common.base.Stopwatch;

import java.io.IOException;

/**
 * Created by Saeed on 8/15/2015.
 */
public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        int size = 4;
        int threads = 1;
        String graph_path = "X:\\networks\\kcminer\\wikivote.txt";
        OldGraph graph;
        Graph ngraph;
        long count;
        Stopwatch stopwatch = Stopwatch.createUnstarted();

        //        stopwatch.reset().start();
        //        graph = OldGraph.buildFromEdgeListFile(graph_path);
        ngraph = Graph.buildFromEdgeListFile(graph_path);
        //        System.out.println(graph.getInfo());
        //        System.out.println("Took:" + stopwatch);
        //        stopwatch.reset().start();
        //        count = CliqueEnumerator.parallelEnumerate(new RawCliqueStateManager(), graph, size, threads, null);
        //        System.out.printf("%,d\n", count);
        //        System.out.println("Took:" + stopwatch);
        //        System.out.println("=========================");
        //
        //
        //        stopwatch.reset().start();
        //        graph = PrunedGraph.buildFromEdgeListFile(graph_path);
        //        System.out.println(graph.getInfo());
        //        System.out.println("Took:" + stopwatch);
        //        stopwatch.reset().start();
        //        count = CliqueEnumerator.parallelEnumerate(new PrunedCliqueStateManager(), graph, size, threads,
        // null);
        //        System.out.printf("%,d\n", count);
        //        System.out.println("Took:" + stopwatch);
        //        System.out.println("=========================");
        //
        //        stopwatch.reset().start();
        //        count = CliqueEnumerator.parallelCount(new PrunedCliqueStateManager(), graph, size, threads);
        //        System.out.printf("%,d\n", count);
        //        System.out.println("Took:" + stopwatch);

        stopwatch.reset().start();
        System.out.println("*************************************************");
        count = NewCliqueEnumerator.parallelCountFixed(ngraph, size, threads);
        System.out.printf("%,d\n", count);
        System.out.println("Took:" + stopwatch);

        stopwatch.reset().start();
        System.out.println("*************************************************");
        count = NewCliqueEnumerator.parallelCountMaximal(ngraph, size, threads);
        System.out.printf("%,d\n", count);
        System.out.println("Took:" + stopwatch);




    }

}
