package amu.saeed.kcminer.smp;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class CliqueState {
    static final int flushLimit = 1024 * 1024;
    int[] subgraph = null;
    int[] extension = null;
    int extSize = 0;

    protected CliqueState() {}

    final public static CliqueState fromInts(int[] array) {
        int index = 0;
        CliqueState state = new CliqueState();

        int count = array[index++];
        state.subgraph = Arrays.copyOfRange(array, index, index + count);
        index += count;

        count = array[index++];
        state.extension = Arrays.copyOfRange(array, index, index + count);
        state.extSize = count;
        return state;
    }

    final public static long parallelCountRecursive(final CliqueStateManager cliqueMan, final Graph graph, final int k,
        final int thread_count) {

        final AtomicLong counter = new AtomicLong();
        final ConcurrentLinkedQueue<Integer> cq = new ConcurrentLinkedQueue<Integer>();
        for (int v : graph.vertices)
            cq.add(v);

        Thread[] threads = new Thread[thread_count];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Runnable() {
                public void run() {
                    while (!cq.isEmpty()) {
                        Integer v = cq.poll();
                        if (v == null)
                            break;
                        counter.addAndGet(
                            cliqueMan.makeNew(v, graph.getNeighbors(v)).countCliquesRecursive(k, graph, cliqueMan));
                    }
                }
            });
            threads[i].start();
        }

        for (int i = 0; i < threads.length; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
        return counter.get();
    }

    static final public long parallelEnumerate(final CliqueStateManager cliqueMan, final Graph graph, final int k,
        final int thread_count, final String path) throws IOException {
        final Object lock = new Object();

        final AtomicLong counter = new AtomicLong();
        final ConcurrentLinkedQueue<Integer> cq = new ConcurrentLinkedQueue<Integer>();
        for (int v : graph.vertices)
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
                        ArrayList<CliqueState> list = new ArrayList<CliqueState>();
                        list.add(cliqueMan.makeNew(v, graph.getNeighbors(v)));
                        while (!list.isEmpty()) {
                            CliqueState state = list.get(list.size() - 1);
                            list.remove(list.size() - 1);
                            if (state.subgraph.length == k - 1) {
                                counter.getAndAdd(state.extSize);
                                if (writer != null)
                                    for (int i = 0; i < state.extSize; i++)
                                        state.expandCliqueAndWriteToStringBuilder(state.extension[i], builder);
                            }
                            if (state.subgraph.length >= k) {
                                counter.getAndIncrement();
                                state.writeCliqueToStringBuilder(builder);
                            }
                            if (state.subgraph.length == k - 1)
                                continue;

                            int w = 0;
                            for (int i = 0; i < state.extSize; i++) {
                                w = state.extension[i];
                                CliqueState new_state = cliqueMan.expand(state, w, graph.getNeighbors(w));
                                if (new_state.subgraph.length + new_state.extSize >= k)
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
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
        if (writer != null)
            writer.close();
        return counter.get();
    }

    final public long countCliquesRecursive(final int k, final Graph graph, final CliqueStateManager cliqueMan) {
        long cliqueCount = 0;
        if (subgraph.length == k)
            cliqueCount = 1L;
        else if (subgraph.length == k - 1)
            cliqueCount = extSize;
        else {
            int w;
            for (int i = 0; i < extSize; i++) {
                w = extension[i];
                CliqueState new_state = cliqueMan.expand(this, w, graph.getNeighbors(w));
                if (new_state.subgraph.length + new_state.extSize >= k)
                    cliqueCount += new_state.countCliquesRecursive(k, graph, cliqueMan);
            }
        }
        return cliqueCount;
    }

    final public String toString() {
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

    public static interface CliqueStateManager {
        CliqueState makeNew(int v, int[] neighbors);

        CliqueState expand(CliqueState state, int w, int[] w_neighbors);
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
    //    public static long parallelCount(final Graph g, final int lower, final int k, final int
    // thread_count)
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
