package amu.saeed.kcminer.smp;

import amu.saeed.kcminer.graph.Graph;
import amu.saeed.kcminer.graph.KCliqueState;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Saeed on 8/18/2015.
 */
public class KCliqueEnumerator {
    static int flushLimit = 1024 * 1024;

    final public static long parallelTurboCount(final Graph graph, final int k, final int thread_count) {
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
                        counter.addAndGet(new KCliqueState(v, graph.getBiggerNeighbors(v), graph.getSmallerNeighbors(v))
                            .countKCliques(k, graph));
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


    static final public long parallelEnumerate(final Graph graph, final int k, final int thread_count,
        final String path) throws IOException {
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
                        ArrayList<KCliqueState> list = new ArrayList<KCliqueState>();
                        list.add(new KCliqueState(v, graph.getBiggerNeighbors(v), graph.getSmallerNeighbors(v)));
                        while (!list.isEmpty()) {
                            KCliqueState state = list.get(list.size() - 1);
                            list.remove(list.size() - 1);
                            if (state.clique.length == k - 1) {
                                counter.getAndAdd(state.extSize);
                                if (writer != null)
                                    for (int i = 0; i < state.extSize; i++) {
                                        for (int x : state.clique)
                                            builder.append(x).append('\t');
                                        builder.append(state.extension[i]).append('\n');
                                    }
                            }
                            if (state.clique.length >= k) {
                                counter.getAndIncrement();
                                state.writeCliqueToStringBuilder(builder);
                            }
                            if (state.clique.length == k - 1)
                                continue;

                            int w = 0;
                            for (int i = 0; i < state.extSize; i++) {
                                w = state.extension[i];
                                KCliqueState new_state = state.expand(w, graph.getBiggerNeighbors(w));
                                if (new_state.clique.length + new_state.extSize >= k)
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

}
