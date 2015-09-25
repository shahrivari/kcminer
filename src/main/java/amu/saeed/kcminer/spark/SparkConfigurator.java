package amu.saeed.kcminer.spark;

import org.apache.spark.SparkConf;

/**
 * Created by Saeed on 9/24/2015.
 */
public class SparkConfigurator {
    public static void config(SparkConf conf) {
        conf.set("spark.executor.memory", "10g");
        conf.set("spark.akka.frameSize", "128");
        conf.set("spark.executor.extraJavaOptions",
            "-XX:+UseParallelGC -XX:+UseParallelOldGC " + "-XX:ParallelGCThreads=6 -XX:MaxGCPauseMillis=100");
        conf.set("spark.storage.memoryFraction", "0.05");
        conf.set("spark.shuffle.memoryFraction", "0.05");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

    }

}
