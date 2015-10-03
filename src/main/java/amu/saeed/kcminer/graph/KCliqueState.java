package amu.saeed.kcminer.graph;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KCliqueState {
    public int[] clique = null;
    public int[] extension = null;
    public int extSize = 0;
    public int w = -1;

    public KCliqueState() {}

    public KCliqueState(int v, int[] bigger_neighbors) {
        clique = new int[] {v};
        extension = bigger_neighbors.clone();
        extSize = extension.length;
    }

    final public static KCliqueState fromIntArray(int[] array) {
        int index = 0;
        KCliqueState state = new KCliqueState();

        int count = array[index++];
        state.clique = Arrays.copyOfRange(array, index, index + count);
        index += count;

        count = array[index++];
        state.extension = Arrays.copyOfRange(array, index, index + count);
        state.extSize = count;
        return state;
    }

    final public KCliqueState expand(int w, int[] wBiggerNeighbors) {
        KCliqueState newState = new KCliqueState();
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

    final public List<KCliqueState> getChildren(final Graph graph) {
        ArrayList<KCliqueState> children = new ArrayList();
        for (int i = 0; i < extSize; i++) {
            int w = extension[i];
            KCliqueState new_state = expand(w, graph.getBiggerNeighbors(w));
            children.add(new_state);
        }
        return children;
    }

    final public long countKCliques(final int k, final Graph graph) {
        long cliqueCount = 0;
        if (clique.length == k)
            cliqueCount = 1L;
        else if (clique.length == k - 1)
            cliqueCount = extSize;
        else {
            int w;
            for (int i = 0; i < extSize; i++) {
                w = extension[i];
                KCliqueState new_state = expand(w, graph.getBiggerNeighbors(w));
                if (new_state.clique.length + new_state.extSize >= k)
                    cliqueCount += new_state.countKCliques(k, graph);
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

    public final void writeToStream(DataOutputStream dos) throws IOException {
        dos.writeInt(clique.length);
        for (int x : clique)
            dos.writeInt(x);
        dos.writeInt(extSize);
        for (int i = 0; i < extSize; i++)
            dos.writeInt(extension[i]);
        dos.writeInt(w);
    }

    final public void readFromStream(DataInputStream dis) throws IOException {
        clique = new int[dis.readInt()];
        for (int i = 0; i < clique.length; i++)
            clique[i] = dis.readInt();

        extension = new int[dis.readInt()];
        extSize = extension.length;
        for (int i = 0; i < extension.length; i++)
            extension[i] = dis.readInt();
        w = dis.readInt();
    }


}
