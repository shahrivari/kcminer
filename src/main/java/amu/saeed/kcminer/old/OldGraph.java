package amu.saeed.kcminer.old;


import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by Saeed on 8/1/14.
 */
public class OldGraph {
    public int[] vertices;
    IntObjectHashMap<int[]> neighbors = new IntObjectHashMap<int[]>();

    public static OldGraph buildFromEdgeListFile(String path) throws IOException {
        OldGraph graph = new OldGraph();
        IntObjectHashMap<IntHashSet> neighbors = new IntObjectHashMap<IntHashSet>();

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
            if (!neighbors.containsKey(src))
                neighbors.put(src, new IntHashSet());
            if (!neighbors.containsKey(dest))
                neighbors.put(dest, new IntHashSet());
            neighbors.get(src).add(dest);
            neighbors.get(dest).add(src);
        }

        graph.vertices = neighbors.keys().toArray();
        Arrays.sort(graph.vertices);

        for (IntObjectCursor<IntHashSet> c : neighbors) {
            int[] array = c.value.toArray();
            Arrays.sort(array);
            graph.neighbors.put(c.key, array);
        }

        br.close();
        return graph;
    }

    public int[] getNeighbors(int v) {
        return neighbors.get(v);
    }

    protected int edgeCount() {
        int edges = 0;
        for (IntObjectCursor<int[]> x : neighbors)
            edges += x.value.length;
        return edges / 2;
    }

    public String getInfo() {
        String info = "#Nodes: " + String.format("%,d", vertices.length) + "\n";
        int edges = edgeCount();
        info += "#Edges: " + String.format("%,d", edges) + "\n";
        info += "AVG(degree): " + String.format("%.2f", edges / (double) vertices.length);
        return info;
    }


}
