package amu.saeed.peco;//package msven;

// Copyright (C) Mike Svendsen
// For questions contact Prof. Srikanta Tirthapura (snt@iastate.edu)

/***
 * @author Michael S Svendsen
 * Iowa State University
 * August 8th, 2012
 * Implementation of Tomita et al. algorithm for Maximal Clique Enumeration
 */
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.*;

public class TomitaSeq {

    private static HashMap<Integer, LinkedList<Integer>> subgraph;
    private static HashMap<Integer, Integer> cliqueCount;
    private static boolean newline;

    /**
     * @param args
     */
    public static void main(String[] args) {

        if (args.length != 1) {
            System.out.println("Tomita <input>");
        }

        subgraph = new HashMap<Integer, LinkedList<Integer>>();
        cliqueCount = new HashMap<Integer, Integer>();
        newline = false;

        // args[0] should be path to file of graph in adjacency list format
        // Neighbors of a particular node should be sorted from smallest to largest in its entry
        createSubgraph(args[0]);
        expand(new HashSet<Integer>(), new HashSet<Integer>(subgraph.keySet()), 0, new LinkedList<Integer>());
        //		System.out.print("\n");
        printResults();

    }

    private static void printResults() {

        Iterator<Integer> cliqueSizes = cliqueCount.keySet().iterator();
        while (cliqueSizes.hasNext()) {
            Integer next = cliqueSizes.next();
            System.out.println("Cliques of size " + next + ": " + cliqueCount.get(next));
        }

    }

    private static void expand(HashSet<Integer> FINI, HashSet<Integer> CAND, int iCcurSize,
        LinkedList<Integer> clique) {

        int currentSize = iCcurSize;

        if (FINI.isEmpty() && CAND.isEmpty())    // Line 2
        {
            //			System.out.print("clique,");
            Integer count = cliqueCount.get(iCcurSize);
            if (count == null) {
                cliqueCount.put(iCcurSize, 1);
            } else {
                cliqueCount.put(iCcurSize, count + 1);
            }
            newline = true;
            System.out.println(clique.toString());
            return;
        } else {
            //Line 4
            Integer u = maxIntersection(FINI, CAND);
            HashSet<Integer> EXT = new HashSet<Integer>(CAND);
            EXT.removeAll(subgraph.get(u));      //EXT = CAND - N(u)

            while (!EXT.isEmpty())    //Line 5
            {
                Integer q = EXT.iterator().next();    //Line 6
                clique.push(q);
                //				if(newline)
                //				{
                //					System.out.print("\n" + q + ",");			//Line 7
                //					newline = false;
                //				}
                //				else
                //					System.out.print("" + q + ",");
                currentSize++;

                //Line 8
                HashSet<Integer> FINIq = new HashSet<Integer>(FINI);
                FINIq.retainAll(subgraph.get(q));

                //Line 9
                HashSet<Integer> CANDq = new HashSet<Integer>(CAND);
                CANDq.retainAll(subgraph.get(q));

                //Line 10
                expand(FINIq, CANDq, currentSize, clique);

                //Line 11
                CAND.remove(q);
                FINI.add(q);
                EXT.remove(q);
                currentSize--;
                clique.pop();

                //Line 12
                //				System.out.print("back,");
            }

        }
        return;

    }


    private static Integer maxIntersection(Set<Integer> FINI, Set<Integer> CAND) {
        Integer max = null;
        int maxInterSize = -1;

        for (Integer u : FINI) {
            LinkedList<Integer> uNeighbor = subgraph.get(u);
            if (maxInterSize == -1)
                max = u;
            if (uNeighbor.isEmpty())
                continue;

            int uSize = uNeighbor.size();
            int candSize = CAND.size();
            int uCount = 0;
            int candCount = 0;
            int intSize = 0;

            Iterator<Integer> uIter = uNeighbor.iterator();
            Iterator<Integer> cIter = CAND.iterator();
            int neigh = -1;
            int cand = -1;

            while (uCount < uSize) {
                if (uCount == 0)  // Base case
                {
                    neigh = uIter.next();
                    if (candCount < candSize)
                        cand = cIter.next();
                    else {
                        uCount = uSize;
                        continue;
                    }
                    uCount++;
                    candCount++;
                }

                if (neigh == cand) {
                    intSize++;
                    if (uCount < uSize) {
                        uCount++;
                        neigh = uIter.next();
                    }
                    if (candCount < candSize) {
                        candCount++;
                        cand = cIter.next();
                    } else  // Last cand matched therefore no more to look at so escape
                    {
                        uCount = uSize;
                    }
                } else if (neigh < cand) {
                    if (uCount < uSize) {
                        uCount++;
                        neigh = uIter.next();
                    }
                } else // cand < neigh
                {
                    if (candCount < candSize) {
                        candCount++;
                        cand = cIter.next();
                    } else  // cand < neigh but out of candidates
                    {
                        uCount = uSize;    //Escape this while loop
                    }
                }


            } //End While

            if (uSize > maxInterSize) {
                maxInterSize = uSize;
                max = u;
            }
        } // End For

        for (Integer u : CAND) {
            LinkedList<Integer> uNeighbor = subgraph.get(u);
            if (maxInterSize == -1)
                max = u;
            if (uNeighbor.isEmpty())
                continue;

            int uSize = uNeighbor.size();
            int candSize = CAND.size();
            int uCount = 0;
            int candCount = 0;
            int intSize = 0;

            Iterator<Integer> uIter = uNeighbor.iterator();
            Iterator<Integer> cIter = CAND.iterator();
            int neigh = -1;
            int cand = -1;

            while (uCount < uSize) {
                if (uCount == 0)  // Base case
                {
                    neigh = uIter.next();
                    if (candCount < candSize)
                        cand = cIter.next();
                    else {
                        uCount = uSize;
                        continue;
                    }
                    uCount++;
                    candCount++;
                }

                if (neigh == cand) {
                    intSize++;
                    if (uCount < uSize) {
                        uCount++;
                        neigh = uIter.next();
                    }
                    if (candCount < candSize) {
                        candCount++;
                        cand = cIter.next();
                    } else  // Last cand matched therefore no more to look at so escape
                    {
                        uCount = uSize;
                    }
                } else if (neigh < cand) {
                    if (uCount < uSize) {
                        uCount++;
                        neigh = uIter.next();
                    }
                } else // cand < neigh
                {
                    if (candCount < candSize) {
                        candCount++;
                        cand = cIter.next();
                    } else  // cand < neigh but out of candidates
                    {
                        uCount = uSize;    //Escape this while loop
                    }
                }


            } //End While

            if (uSize > maxInterSize) {
                maxInterSize = uSize;
                max = u;
            }
        } // End For

        return max;
    }

    //File should be in the form of an adjacency list
    private static void createSubgraph(String filename) {

        FileInputStream in;
        BufferedReader entry = null;
        try {
            in = new FileInputStream(filename);
            entry = new BufferedReader(new InputStreamReader(in));
        } catch (FileNotFoundException e) {
            System.out.println("Could not open input file: " + filename);
            e.printStackTrace();
            System.exit(-1);
        }

        boolean done = false;
        while (!done) {
            String next = null;
            try {
                next = entry.readLine();
            } catch (Exception e) {
                // Do nothing
            }
            if (next == null) {
                done = true;
            } else {

                StringTokenizer token = new StringTokenizer(next);
                Integer vertex = Integer.parseInt(token.nextToken());
                LinkedList<Integer> neighbors = new LinkedList<Integer>();
                while (token.hasMoreTokens()) {
                    neighbors.add(Integer.parseInt(token.nextToken()));
                }

                subgraph.put(vertex, neighbors);
            }
        }
    }

}
