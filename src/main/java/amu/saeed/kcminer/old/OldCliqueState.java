package amu.saeed.kcminer.old;

import java.util.Arrays;

public class OldCliqueState {
    public int[] clique = null;
    public int[] extension = null;
    public int extSize = 0;

    public OldCliqueState() {}

    final public static OldCliqueState fromInts(int[] array) {
        int index = 0;
        OldCliqueState state = new OldCliqueState();

        int count = array[index++];
        state.clique = Arrays.copyOfRange(array, index, index + count);
        index += count;

        count = array[index++];
        state.extension = Arrays.copyOfRange(array, index, index + count);
        state.extSize = count;
        return state;
    }

    final public long countCliquesRecursive(final int k, final OldGraph graph, final OldCliqueStateManager cliqueMan) {
        long cliqueCount = 0;
        if (clique.length == k)
            cliqueCount = 1L;
        else if (clique.length == k - 1)
            cliqueCount = extSize;
        else {
            int w;
            for (int i = 0; i < extSize; i++) {
                w = extension[i];
                OldCliqueState new_state = cliqueMan.expand(this, w, graph.getNeighbors(w));
                if (new_state.clique.length + new_state.extSize >= k)
                    cliqueCount += new_state.countCliquesRecursive(k, graph, cliqueMan);
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
        int[] array = new int[clique.length + extSize + 2];
        int index = 0;
        array[index++] = clique.length;
        for (int x : clique)
            array[index++] = x;
        array[index++] = extSize;
        for (int i = 0; i < extSize; i++)
            array[index++] = extension[i];
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
