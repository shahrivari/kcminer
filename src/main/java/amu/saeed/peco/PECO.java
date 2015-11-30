package amu.saeed.peco;// Copyright (C) Mike Svendsen and Arko Provo Mukherjee
// For questions contact Prof. Srikanta Tirthapura (snt@iastate.edu)

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Date;
import java.util.Formatter;

public class PECO extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PECO(), args);
        System.exit(res);
    }

    @Override public int run(String[] args) throws Exception {
        if (args.length < 3) {

            System.out.println("MC <inDir> <outDir> <cliqueSize> <numReducer> [Options... -c -cm -iO -rO -dO -wu]");
            System.out.println("-c\tCompress output");
            System.out.println("-cm\tCompress map output");
            System.out.println("-iO\tVertex Id ordering (Asscending");
            System.out.println("-rO\tRandom vertex ordering");
            System.out.println("-dO\tDegree vertex ordering (DEFAULT)");
            System.out.println("-wu\tUse Wu Alg");
            return -1;
        }
        String strArgs = " ";
        for (int i = 0; i < args.length; i++) {
            strArgs += args[i] + " ";
        }

        String inDir = args[0];          // Get input directory
        String outDir = args[1];          // Get output directory
        int k = Integer.parseInt(args[2]);  // Get subSize
        int numReduce = Integer.parseInt(args[3]);  // Get num reduce tasks


        long start = System.currentTimeMillis();

        Configuration conf = getConf();
        conf.addResource("properties.xml");      //Add Resource file
        conf.setInt("k", k);

        // Use a random ordering over vertices
        if (strArgs.contains(" -rO ")) {
            conf.set("RandomOrder", Integer.toString(1));
        } else {
            conf.set("RandomOrder", Integer.toString(0));
        }

        // Use a degree ordering ( Default - Recommended)
        if (strArgs.contains(" -dO ")) {
            conf.set("DegreeOrder", Integer.toString(1));
        } else {
            conf.set("DegreeOrder", Integer.toString(0));
        }

        boolean wuAlg = false;
        if (strArgs.contains(" -wu ")) {
            wuAlg = true;
            conf.set("WuAlg", Integer.toString(1));  // Use Wu et al Alg
        } else {
            conf.set("WuAlg", Integer.toString(0));
        }


        if (strArgs.contains(" -c "))            //Compress output of this job
        {
            conf.setBoolean("mapred.output.compress", true);
            conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);
        }

        if (strArgs.contains(" -cm ")) {
            conf.setBoolean("mapred.compress.map.output", true);
            conf.setClass("mapred.map.output.compression.codec", GzipCodec.class, CompressionCodec.class);
        }


        /*** Run Maximal Clique Main Job ***/
        Job job = new Job(conf, "PECO-" + inDir + "-" + k);
        job.setJarByClass(PECO.class);

        job.setNumReduceTasks(numReduce);        //Set number of reduce tasks

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(LongWritable.class);  //Set Map output key class
        job.setMapOutputValueClass(Text.class);      //Set Map output value class

        job.setOutputKeyClass(LongWritable.class);    //Set reduce output key class
        job.setOutputValueClass(Text.class);      //Set reduce output value class

        Formatter formatter = new Formatter();
        String toAppend = "_" + formatter.format("%1$tm%1$td%1$tH%1$tM%1$tS", new Date());
        Path output = new Path(outDir + toAppend);
        FileInputFormat.setInputPaths(job, new Path(inDir));    //Set input directory
        FileOutputFormat.setOutputPath(job, output);        //Set output directory
        formatter.close();

        //Set Mapper and Reducer Class
        if (wuAlg) {
            job.setMapperClass(WuMap.class);
            job.setReducerClass(WuRed.class);
        } else {
            job.setMapperClass(DegreeMap.class);
            job.setReducerClass(DegreeRed.class);
        }

        //Run Job
        boolean success = job.waitForCompletion(true);
        if (success) {
            System.out.println("Job Complete");

            long end = System.currentTimeMillis(); // Mark the end time

            System.out.println("The program terminated in " + ((end - start) / 1000) + "secs.");
        } else {
            System.out.println("Job Failed");
            return -2;
        }

        return 0;
    }
}
