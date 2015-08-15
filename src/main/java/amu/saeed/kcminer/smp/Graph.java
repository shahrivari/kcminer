package amu.saeed.kcminer.smp;


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
public class Graph {
    int[] vertices;
    IntObjectHashMap<int[]> neighbors = new IntObjectHashMap<int[]>();

    public static Graph buildFromEdgeListFile(String path) throws IOException {
        Graph table = new Graph();
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

        table.vertices = neighbors.keys().toArray();
        Arrays.sort(table.vertices);

        for (IntObjectCursor<IntHashSet> c : neighbors) {
            int[] array = c.value.toArray();
            Arrays.sort(array);
            table.neighbors.put(c.key, array);
        }

        br.close();
        return table;
    }

    public int[] getNeighbors(int v) {
        return neighbors.get(v);
    }

    public String getInfo() {
        String info = "#Nodes: " + String.format("%,d", vertices.length) + "\n";
        long edges = 0;
        for (IntObjectCursor<int[]> x : neighbors)
            edges += x.value.length;
        edges = edges / 2;
        info += "#Edges: " + String.format("%,d", edges) + "\n";
        info += "AVG(degree): " + String.format("%.2f", edges / (double) vertices.length);
        return info;
    }


}
