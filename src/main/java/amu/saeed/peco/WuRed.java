package amu.saeed.peco;// Copyright (C) Mike Svendsen and Arko Provo Mukherjee
// For questions contact Prof. Srikanta Tirthapura (snt@iastate.edu)

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class WuRed extends Reducer<LongWritable, Text, LongWritable, Text> {
    //Keys of hashmap are neighbors to v
    //Values associated with key u are the neighbors of u
    private HashMap<LongWritable, LinkedList<LongWritable>> SubGraph;
    private HashMap<Long, Long> vertexOrder;    // Ordering of vertices

    @Override protected void reduce(LongWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        //Key = a node v
        //Values = neighbor u of v + neighbors of u
        String strCcur = key.toString();  //Current clique being processed

        SubGraph = new HashMap<LongWritable, LinkedList<LongWritable>>();
        vertexOrder = new HashMap<Long, Long>();

        // Add the keys rank to the ordering
        vertexOrder.put(key.get(), key.get());

        Iterator<Text> iter = values.iterator();
        while (iter.hasNext()) {
            String line = iter.next().toString();  //Get line

            //System.out.println("Recevied: " + key.toString() + " | " + line);
            StringTokenizer tokenLine = new StringTokenizer(line);      //Split into tokens
            String firstToken = tokenLine.nextToken();

            if (firstToken.equals("n")) {
                // Line has v gamma(v)
                LongWritable v = new LongWritable(Long.parseLong(tokenLine.nextToken()));
                vertexOrder.put(v.get(), v.get());
                LinkedList<LongWritable> neigh = SubGraph.get(v);
                if (neigh == null) {
                    neigh = new LinkedList<LongWritable>();
                }
                while (tokenLine.hasMoreTokens()) {
                    LongWritable next = new LongWritable(Long.parseLong(tokenLine.nextToken()));
                    vertexOrder.put(next.get(), next.get());
                    neigh.add(next);
                    LinkedList<LongWritable> uNeigh = SubGraph.get(next);
                    if (uNeigh == null) {
                        uNeigh = new LinkedList<LongWritable>();
                    }
                    uNeigh.add(v);
                    SubGraph.put(next, uNeigh);
                }
                SubGraph.put(v, neigh);
            } else {
                // Else standard line of v u gamma(u)
                LongWritable vNeighbor = new LongWritable(Long.parseLong(firstToken));  //First token is neighbor of v;

                vertexOrder.put(vNeighbor.get(), vNeighbor.get());
                long ivNeighbor = vNeighbor.get();

                LinkedList<LongWritable> vTwoHop = SubGraph.get(vNeighbor);
                if (vTwoHop == null)
                    vTwoHop = new LinkedList<LongWritable>();            //Rest of tokens are neighbors of vNeighbor

                while (tokenLine.hasMoreTokens()) {
                    LongWritable next = new LongWritable(Long.parseLong(tokenLine.nextToken()));
                    long iNext = next.get();
                    if (iNext != ivNeighbor)
                        vTwoHop.add(next);              //Mapper included u in the list of u's neighbors

                    vertexOrder.put(iNext, iNext);

                    // In Wu Alg each edge is only received once, so need to update the neighbors adj entry as well
                    LinkedList<LongWritable> neigh = SubGraph.get(next);
                    if (neigh == null) {
                        neigh = new LinkedList<LongWritable>();
                    }
                    neigh.add(vNeighbor);
                    SubGraph.put(next, neigh);
                }
                SubGraph.put(vNeighbor, vTwoHop);
            }

        }

        HashSet<LongWritable> neighbors = new HashSet<LongWritable>(SubGraph.get(key));

        //TODO:  We may be able to ignore neighbors of key
        HashSet<LongWritable> CAND = new HashSet<LongWritable>(neighbors);
        HashSet<LongWritable> FINI = new HashSet<LongWritable>();
        CAND.remove(key);
        // Remove any vertices which are not direct neighbors
        neighbors.remove(key);
        for (LongWritable u : neighbors) {
            LinkedList<LongWritable> uNeighbors = SubGraph.get(u);
            uNeighbors.retainAll(neighbors);
            if (vertexOrder.get(u.get()) > vertexOrder.get(key.get()) || (
                vertexOrder.get(u.get()).equals(vertexOrder.get(key.get())) && u.get() > key.get())) {
                CAND.remove(u);
                FINI.add(u);
            }
        }

        expand(FINI, CAND, key, strCcur, 1, context);    //Find all cliques key is part of
    }

    /**
     * Find all cliques that strCcur is involved in
     *
     * @param SUBG      - All neighbors u along with u's neighbors, such that u is a neighbor of every node in the
     *                  current clique
     * @param CAND      - The most recently added node to the current clique
     * @param strCcur   - A string containing the nodes in the current clique
     * @param iCcurSize - size of the current clique
     * @param reporter  - Reporter used for reporting size and number of cliques found
     * @return The list of all cliques found after the call to this function
     * @throws InterruptedException
     * @throws IOException
     */
    private void expand(Set<LongWritable> FINI, Set<LongWritable> CAND, LongWritable key, String strCcur, int iCcurSize,
        Context context) throws IOException, InterruptedException {

        context.setStatus("W" + iCcurSize);  //report progress so task isn't deemed failed.

        //Create images of current state
        String strCcurImage = new String(strCcur);

        int currentSize = iCcurSize;

        if (FINI.isEmpty() && CAND.isEmpty())    // Line 2
        {
            //strCall += "\n\t" + strCcur;		//Only output cliques larger than min_clique_size
            report_size(context, currentSize);
            context.write(key, new Text(strCcur));
            return;// strCall;
        } else if (CAND.isEmpty()) {
            return;// strCall;
        } else {
            //Line 4
            HashSet<LongWritable> EXT = new HashSet<LongWritable>(CAND);
            //HashSet<LongWritable> FINI = new HashSet<LongWritable>();

            while (!EXT.isEmpty())    //Line 5
            {
                LongWritable q = EXT.iterator().next();    //Line 6
                strCcur += " " + q.toString();        //Line 7
                currentSize++;

                //Line 8
                HashSet<LongWritable> FINIq = new HashSet<LongWritable>(FINI);
                FINIq.retainAll(SubGraph.get(q));

                //Line 9
                HashSet<LongWritable> CANDq = new HashSet<LongWritable>(CAND);
                CANDq.retainAll(SubGraph.get(q));

                //Line 10
                expand(FINIq, CANDq, key, strCcur, currentSize, context);

                //Line 11
                CAND.remove(q);
                FINI.add(q);
                EXT.remove(q);
                currentSize--;

                //Line 12
                strCcur = strCcurImage;
            }

        }
        return;
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
