package amu.saeed.peco;// Copyright (C) Mike Svendsen and Arko Provo Mukherjee
// For questions contact Prof. Srikanta Tirthapura (snt@iastate.edu)

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

//import org.apache.hadoop.io.IntWritable;


public class DegreeRed extends Reducer<LongWritable, Text, LongWritable, Text> {

    private HashMap<LongWritable, LinkedList<LongWritable>> SubGraph;
    private int k;

    @Override public void setup(Context context) {
        Configuration conf = context.getConfiguration();
        k = conf.getInt("k", 3);
    }

    @Override protected void reduce(LongWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

        SubGraph = new HashMap<LongWritable, LinkedList<LongWritable>>();

        Iterator<Text> iter = values.iterator();
        while (iter.hasNext()) {
            String line = iter.next().toString();  //Get line
            StringTokenizer tokenLine = new StringTokenizer(line);      //Split into tokens
            LongWritable vNeighbor =
                new LongWritable(Long.parseLong(tokenLine.nextToken()));  //First token is neighbor of v;


            long ivNeighbor = vNeighbor.get();

            LinkedList<LongWritable> vTwoHop =
                new LinkedList<LongWritable>();  //Rest of tokens are neighbors of vNeighbor
            while (tokenLine.hasMoreTokens()) {
                LongWritable next = new LongWritable(Long.parseLong(tokenLine.nextToken()));
                long iNext = next.get();
                if (iNext > ivNeighbor)
                    vTwoHop.add(next);    //Mapper included u in the list of u's neighbors
            }
            SubGraph.put(vNeighbor, vTwoHop);
        }

        //        //TODO:  We may be able to ignore neighbors of key
        HashSet<LongWritable> CLIQ = new HashSet<LongWritable>();
        CLIQ.add(key);
        HashSet<LongWritable> CAND = new HashSet<LongWritable>();
        for (LongWritable w : SubGraph.keySet())
            if (w.get() > key.get())
                CAND.add(w);

        findKCliques(CLIQ, CAND, context);
    }

    private void findKCliques(Set<LongWritable> CLIQ, Set<LongWritable> CAND, Context context) {
        context.setStatus("W" + CLIQ.size());

        if (CLIQ.size() > k)
            return;
        report_size(context, CLIQ.size());
        if (CLIQ.size() == k || CAND.size() == 0)
            return;

        LongWritable[] ws = new LongWritable[CAND.size()];
        CAND.toArray(ws);

        HashSet<LongWritable> NCAND = new HashSet<>();
        for (LongWritable w : ws) {
            NCAND.clear();
            NCAND.addAll(CAND);
            NCAND.remove(w);
            CLIQ.add(w);

            LinkedList<LongWritable> wNeighbors = SubGraph.get(w);
            if (wNeighbors == null)
                throw new IllegalStateException("The neighbors of w was not found!");

            NCAND.retainAll(wNeighbors);

            findKCliques(CLIQ, NCAND, context);
            CLIQ.remove(w);
        }
    }

    /**
     * Report the size of the clique
     *
     * @param context     - Reporter for reporting the size of the current clique
     * @param currentSize - Size of the clique being reported
     */
    private void report_size(Context context, int currentSize) {
        String counterName = "Clique of size ";
        counterName += currentSize;

        Counter counter = context.getCounter("CliqueSizeAndNum", counterName);
        counter.increment(1);

    }
}
