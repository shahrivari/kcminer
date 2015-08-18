package amu.saeed.kcminer.graph;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by Saeed on 8/18/2015.
 */
public class Graph {
    public int[] vertices;
    IntObjectHashMap<int[]> biggerNeighbors = new IntObjectHashMap<int[]>();
    IntObjectHashMap<int[]> smallerNeighbors = new IntObjectHashMap<int[]>();

    public static Graph buildFromEdgeListFile(String path) throws IOException {
        Graph graph = new Graph();
        IntObjectHashMap<IntHashSet> largerNeighbors = new IntObjectHashMap<IntHashSet>();
        IntObjectHashMap<IntHashSet> smallerNeighbors = new IntObjectHashMap<IntHashSet>();

        BufferedReader br = new BufferedReader(new FileReader(path));
        String line;
        while ((line = br.readLine()) != null) {
            if (line.isEmpty())
                continue;
            if (line.startsWith("#")) {
                System.err.printf("Skipped a line: [%s]\n", line);
                continue;
            }
            String[] tokens = line.split("\\s+");
            if (tokens.length < 2) {
                System.err.printf("Skipped a line: [%s]\n", line);
                continue;
            }
            int src = Integer.parseInt(tokens[0]);
            int dest = Integer.parseInt(tokens[1]);
            if (!largerNeighbors.containsKey(src))
                largerNeighbors.put(src, new IntHashSet());
            if (!largerNeighbors.containsKey(dest))
                largerNeighbors.put(dest, new IntHashSet());
            if (!smallerNeighbors.containsKey(src))
                smallerNeighbors.put(src, new IntHashSet());
            if (!smallerNeighbors.containsKey(dest))
                smallerNeighbors.put(dest, new IntHashSet());

            if (dest > src)
                largerNeighbors.get(src).add(dest);
            else
                smallerNeighbors.get(src).add(dest);
            if (src > dest)
                largerNeighbors.get(dest).add(src);
            else
                smallerNeighbors.get(dest).add(src);
        }

        graph.vertices = largerNeighbors.keys().toArray();
        Arrays.sort(graph.vertices);

        for (IntObjectCursor<IntHashSet> c : largerNeighbors) {
            int[] array = c.value.toArray();
            Arrays.sort(array);
            graph.biggerNeighbors.put(c.key, array);
        }

        for (IntObjectCursor<IntHashSet> c : smallerNeighbors) {
            int[] array = c.value.toArray();
            Arrays.sort(array);
            graph.smallerNeighbors.put(c.key, array);
        }

        br.close();
        return graph;
    }

    public int[] getBiggerNeighbors(int v) {
        return biggerNeighbors.get(v);
    }

    public int[] getSmallerNeighbors(int v) {
        return smallerNeighbors.get(v);
    }

    protected int edgeCount() {
        int edges = 0;
        for (IntObjectCursor<int[]> x : biggerNeighbors)
            edges += x.value.length;
        return edges;
    }

    public String getInfo() {
        String info = "#Nodes: " + String.format("%,d", vertices.length) + "\n";
        int edges = edgeCount();
        info += "#Edges: " + String.format("%,d", edges) + "\n";
        info += "AVG(degree): " + String.format("%.2f", edges / (double) vertices.length);
        return info;
    }

}
