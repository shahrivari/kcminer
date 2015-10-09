package amu.saeed.peco;// Copyright (C) Mike Svendsen and Arko Provo Mukherjee
// For questions contact Prof. Srikanta Tirthapura (snt@iastate.edu)

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class DegreeMap extends Mapper<LongWritable, Text, LongWritable, Text> {

    /**
     * This function is automatically called when configuring Map task
     *
     * @param Context context
     */
    @Override public void setup(Context context) {
        // Configuration conf = context.getConfiguration();
        // Access conf data
    }

    /**
     * @param LongWritable key - Line number of the line of input currently being mapped
     * @param Text         value - The line of input corresponding to the line number stored in key
     * @param Context      context
     * @throws InterruptedException
     * @throws IOException
     */
    @Override protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        String input = value.toString();            //Get line
        StringTokenizer tokens = new StringTokenizer(input);  //Tokenize the line


        //Read first value to get node - rest of line are the neighbors of node
        // LongWritable nodeV = new LongWritable(Long.parseLong(tokens.nextToken()));
        tokens.nextToken();  // Read nodeV from line

        while (tokens.hasMoreTokens()) {
            //for each neighbor of node emit, u node+neighbors
            LongWritable nodeU = new LongWritable(Long.parseLong(tokens.nextToken()));
            context
                .write(nodeU, value);  //emit (nodeU, nodeV + neighborsV) Note nodeU is still in this list of neighbors
        }
    }
}
