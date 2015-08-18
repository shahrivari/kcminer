package amu.saeed.kcminer.graph;

import java.util.Arrays;

public class CliqueState {
    public int[] clique = null;
    public int[] extension = null;
    public int[] tabu = null;
    public int extSize = 0;
    public int tabuSize = 0;

    private CliqueState() {}

    public CliqueState(int v, int[] bigger_neighbors, int[] smaller_neighbors) {
        clique = new int[] {v};
        extension = bigger_neighbors.clone();
        extSize = extension.length;
        tabu = smaller_neighbors.clone();
        tabuSize = tabu.length;
    }

    final public static CliqueState fromIntArray(int[] array) {
        int index = 0;
        CliqueState state = new CliqueState();

        int count = array[index++];
        state.clique = Arrays.copyOfRange(array, index, index + count);
        index += count;

        count = array[index++];
        state.extension = Arrays.copyOfRange(array, index, index + count);
        state.extSize = count;

        count = array[index++];
        state.tabu = Arrays.copyOfRange(array, index, index + count);
        state.tabuSize = count;

        return state;
    }

    final public CliqueState expandFixed(int w, int[] wBiggerNeighbors) {
        CliqueState newState = new CliqueState();
        newState.clique = new int[clique.length + 1];
        System.arraycopy(clique, 0, newState.clique, 0, clique.length);
        newState.clique[clique.length] = w;
        newState.extension = new int[extSize];
        int i = 0, j = 0;
        while (i < extSize && j < wBiggerNeighbors.length) {
            if (extension[i] < wBiggerNeighbors[j])
                i++;
            else if (extension[i] > wBiggerNeighbors[j])
                j++;
            else {
                newState.extension[newState.extSize++] = extension[i];
                i++;
                j++;
            }
        }
        return newState;
    }

    final public CliqueState expandMaximal(int w, int[] wBiggerNeighbors, int[] wSmallerNeighbors) {
        CliqueState newState = new CliqueState();
        newState.clique = new int[clique.length + 1];
        System.arraycopy(clique, 0, newState.clique, 0, clique.length);
        newState.clique[clique.length] = w;
        newState.extension = new int[extSize];
        int i = 0, j = 0;
        while (i < extSize && j < wBiggerNeighbors.length) {
            if (extension[i] < wBiggerNeighbors[j])
                i++;
            else if (extension[i] > wBiggerNeighbors[j])
                j++;
            else {
                newState.extension[newState.extSize++] = extension[i];
                i++;
                j++;
            }
        }

        newState.tabu = new int[tabuSize];
        i = 0;
        j = 0;
        while (i < tabuSize && j < wSmallerNeighbors.length) {
            if (tabu[i] < wSmallerNeighbors[j])
                i++;
            else if (tabu[i] > wSmallerNeighbors[j])
                j++;
            else {
                newState.tabu[newState.tabuSize++] = tabu[i];
                i++;
                j++;
            }
        }
        return newState;
    }

    final public long countFixedCliques(final int k, final Graph graph) {
        long cliqueCount = 0;
        if (clique.length == k)
            cliqueCount = 1L;
        else if (clique.length == k - 1)
            cliqueCount = extSize;
        else {
            int w;
            for (int i = 0; i < extSize; i++) {
                w = extension[i];
                CliqueState new_state = expandFixed(w, graph.getBiggerNeighbors(w));
                if (new_state.clique.length + new_state.extSize >= k)
                    cliqueCount += new_state.countFixedCliques(k, graph);
            }
        }
        return cliqueCount;
    }

    final public long countMaximalCliques(final int k, final Graph graph) {
        long cliqueCount = 0;
        if (clique.length == k) {
            if (extSize == 0 && tabuSize == 0)
                cliqueCount = 1L;
        } else {
            int w;
            for (int i = 0; i < extSize; i++) {
                w = extension[i];
                CliqueState new_state = expandMaximal(w, graph.getBiggerNeighbors(w), graph.getSmallerNeighbors(w));
                if (new_state.clique.length + new_state.extSize >= k)
                    cliqueCount += new_state.countMaximalCliques(k, graph);
            }
        }
        return cliqueCount;
    }

    final public String toString() {
        StringBuilder b = new StringBuilder();
        b.append('[');
        if (clique.length == 0)
            b.append(']');
        else
            for (int i = 0; ; i++) {
                b.append(clique[i]);
                if (i == clique.length - 1) {
                    b.append(']').toString();
                    break;
                }
                b.append(", ");
            }
        b.append("->");
        b.append('[');
        if (extSize == 0)
            b.append(']');
        else
            for (int i = 0; ; i++) {
                b.append(extension[i]);
                if (i == extSize - 1) {
                    b.append(']').toString();
                    break;
                }
                b.append(", ");
            }

        return b.toString();
    }

    public final int[] toIntArray() {
        int[] array = new int[clique.length + extSize + tabuSize + 3];
        int index = 0;
        array[index++] = clique.length;
        for (int x : clique)
            array[index++] = x;
        array[index++] = extSize;
        for (int i = 0; i < extSize; i++)
            array[index++] = extension[i];
        array[index++] = tabuSize;
        for (int i = 0; i < tabuSize; i++)
            array[index++] = tabu[i];
        return array;
    }

    private final void expandCliqueAndWriteToStringBuilder(int lastVertex, StringBuilder sb) {
        for (int v : clique)
            sb.append(v).append('\t');
        sb.append(lastVertex).append('\n');
    }

    public final void writeCliqueToStringBuilder(StringBuilder sb) {
        if (clique.length == 0)
            sb.append('\n');
        else
            for (int i = 0; ; i++) {
                sb.append(clique[i]);
                if (i == clique.length - 1) {
                    sb.append('\n');
                    break;
                }
                sb.append('\t');
            }
    }
}
