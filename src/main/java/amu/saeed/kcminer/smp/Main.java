package amu.saeed.kcminer.smp;

import amu.saeed.kcminer.graph.Graph;
import com.google.common.base.Stopwatch;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by Saeed on 8/15/2015.
 */
public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        Params params = new Params();
        try {
            new CmdLineParser(params).parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            new CmdLineParser(params).printUsage(System.err);
            return;
        }
        Stopwatch stopwatch = Stopwatch.createStarted();
        Graph graph = Graph.buildFromEdgeListFile(params.inputPath);
        System.out.printf("Loaded the graph in %s.\n", stopwatch.toString());
        stopwatch.reset().start();

        long count;
        if (params.turboCount)
            count = KCliqueEnumerator.parallelTurboCount(graph, params.k, params.numThreads);
        else
            count = KCliqueEnumerator.parallelEnumerate(graph, params.k, params.numThreads, null);
        System.out.printf("Graph: %s   size: %d   threads: %d   count: %,d\t time: %,d\n",
            new File(params.inputPath).getName(), params.k, params.numThreads, count,
            stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }


    private static class Params {
        @Option(name = "-k", usage = "size of the cliques to mine.", required = true) int k;
        @Option(name = "-t", usage = "number of threads to launch", required = true) int numThreads;
        @Option(name = "-i", usage = "the input path", required = true) String inputPath;
        @Option(name = "-c", usage = "Turbo count") boolean turboCount = false;
    }

}
