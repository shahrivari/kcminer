package amu.saeed.kcminer.smp;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class KlikState {
    int[] subgraph = null;
    int[] extension = null;
    int extSize = 0;


    private KlikState() {
    }

    public KlikState(int v, int[] neighbors) {
        subgraph = new int[]{v};
        extension = new int[neighbors.length];
        for (int i = 0; i < neighbors.length; i++)
            if (neighbors[i] > v)
                extension[extSize++] = neighbors[i];
    }

    public static KlikState fromInts(int[] array) {
        int index = 0;
        KlikState state = new KlikState();

        int count = array[index++];
        state.subgraph = Arrays.copyOfRange(array, index, index + count);
        index += count;

        count = array[index++];
        state.extension = Arrays.copyOfRange(array, index, index + count);
        state.extSize = count;
        return state;
    }

    public static long parallelEnumerate(final Graph g, final int lower, final int k,
                                         final int thread_count, final String path)
            throws IOException, InterruptedException {
        final int flushLimit = 1024 * 1024;
        final Object lock = new Object();

        final AtomicLong counter = new AtomicLong();
        final ConcurrentLinkedQueue<Integer> cq = new ConcurrentLinkedQueue<Integer>();
        for (int v : g.vertices)
            cq.add(v);

        FileWriter writer1 = null;
        if (path != null)
            writer1 = new FileWriter(path);
        final FileWriter writer = writer1;

        Thread[] threads = new Thread[thread_count];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Runnable() {
                public void run() {
                    final StringBuilder builder = new StringBuilder(flushLimit);
                    while (!cq.isEmpty()) {
                        Integer v = cq.poll();
                        if (v == null)
                            break;
                        ArrayList<KlikState> list = new ArrayList<KlikState>();
                        list.add(new KlikState(v, g.getNeighbors(v)));
                        while (!list.isEmpty()) {
                            KlikState state = list.get(list.size() - 1);
                            list.remove(list.size() - 1);
                            if (state.subgraph.length == k - 1) {
                                counter.getAndAdd(state.extSize);
                                if (writer != null)
                                    for (int i = 0; i < state.extSize; i++)
                                        state.expandCliqueAndWriteToStringBuilder(state.extension[i], builder);
                            }
                            if (state.subgraph.length >= lower) {
                                counter.getAndIncrement();
                                state.writeCliqueToStringBuilder(builder);
                            }
                            if (state.subgraph.length == k - 1)
                                continue;

                            int w = 0;
                            for (int i = 0; i < state.extSize; i++) {
                                w = state.extension[i];
                                KlikState new_state = state.expand(w, g.getNeighbors(w));
                                if (new_state.subgraph.length + new_state.extSize >= lower)
                                    list.add(new_state);
                            }
                            if (builder.length() > flushLimit && writer != null) {
                                synchronized (lock) {
                                    try {
                                        writer.write(builder.toString());
                                        builder.setLength(0);
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                        System.exit(-1);
                                    }
                                }
                            }
                        }
                    }
                }
            });
            threads[i].start();
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }
        if (writer != null)
            writer.close();
        return counter.get();
    }

    public final KlikState expand(int w, int[] w_neighbors) {
        KlikState newState = new KlikState();
        newState.subgraph = new int[subgraph.length + 1];
        System.arraycopy(subgraph, 0, newState.subgraph, 0, subgraph.length);
        newState.subgraph[subgraph.length] = w;
        newState.extension = new int[extSize];
        makeNewExt(newState, w, w_neighbors);
        return newState;
    }

    private final void makeNewExt(KlikState newState, int w, int[] w_neighbors) {
        int i = 0, j = 0;
        while (i < extSize && j < w_neighbors.length) {
            if (extension[i] < w_neighbors[j])
                i++;
            else if (extension[i] > w_neighbors[j])
                j++;
            else {
                if (extension[i] > w)
                    newState.extension[newState.extSize++] = extension[i];
                i++;
                j++;
            }
        }
    }

    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append('[');
        if (subgraph.length == 0)
            b.append(']');
        else
            for (int i = 0; ; i++) {
                b.append(subgraph[i]);
                if (i == subgraph.length - 1) {
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
        int[] array = new int[subgraph.length + extSize + 2];
        int index = 0;
        array[index++] = subgraph.length;
        for (int x : subgraph)
            array[index++] = x;
        array[index++] = extSize;
        for (int i = 0; i < extSize; i++)
            array[index++] = extension[i];
        return array;
    }

    private final void expandCliqueAndWriteToStringBuilder(int lastVertex, StringBuilder sb) {
        for (int v : subgraph)
            sb.append(v).append('\t');
        sb.append(lastVertex).append('\n');
    }

    private final void writeCliqueToStringBuilder(StringBuilder sb) {
        if (subgraph.length == 0)
            sb.append('\n');
        else
            for (int i = 0; ; i++) {
                sb.append(subgraph[i]);
                if (i == subgraph.length - 1) {
                    sb.append('\n');
                    break;
                }
                sb.append('\t');
            }
    }


//    public static long count(Graph g, int l, int k) {
//        long count = 0;
//        Stack<KlikState> q = new Stack<KlikState>();
//        for (int v : g.vertices)
//            q.add(new KlikState(v, g.getNeighbors(v)));
//
//        while (!q.isEmpty()) {
//            KlikState state = q.pop();
//            if (state.subgraph.length == k - 1)
//                count += state.extSize;
//
//            if (state.subgraph.length >= l)
//                count++;
//            if (state.subgraph.length == k - 1)
//                continue;
//
//            int w = 0;
//            for (int i = 0; i < state.extSize; i++) {
//                w = state.extension[i];
//                KlikState new_state = state.expand(w, g.getNeighbors(w));
//                if (new_state.subgraph.length + new_state.extSize >= l)
//                    q.add(new_state);
//            }
//        }
//        return count;
//    }
//
//    public static long parallelCount(final Graph g, final int lower, final int k, final int thread_count)
//            throws IOException, InterruptedException {
//        final AtomicLong counter = new AtomicLong();
//        final ConcurrentLinkedQueue<Integer> cq = new ConcurrentLinkedQueue<Integer>();
//        for (int v : g.vertices)
//            cq.add(v);
//
//        Thread[] threads = new Thread[thread_count];
//        for (int i = 0; i < threads.length; i++) {
//            threads[i] = new Thread(new Runnable() {
//                public void run() {
//                    while (!cq.isEmpty()) {
//                        Integer v = cq.poll();
//                        if (v == null)
//                            break;
//                        Stack<KlikState> stack = new Stack<KlikState>();
//                        stack.add(new KlikState(v, g.getNeighbors(v)));
//                        while (!stack.isEmpty()) {
//                            KlikState state = stack.pop();
//                            if (state.subgraph.length == k - 1)
//                                counter.getAndAdd(state.extSize);
//                            if (state.subgraph.length >= lower)
//                                counter.getAndIncrement();
//                            if (state.subgraph.length == k - 1)
//                                continue;
//
//                            int w = 0;
//                            for (int i = 0; i < state.extSize; i++) {
//                                w = state.extension[i];
//                                KlikState new_state = state.expand(w, g.getNeighbors(w));
//                                if (new_state.subgraph.length + new_state.extSize >= lower)
//                                    stack.add(new_state);
//                            }
//                        }
//                    }
//                }
//            });
//            threads[i].start();
//        }
//
//        for (int i = 0; i < threads.length; i++) {
//            threads[i].join();
//        }
//        return counter.get();
//    }

}
