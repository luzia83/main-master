package org.mbd.m9;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.List;

public class WordCountCompact {
    static Logger log = Logger.getLogger(WordCountFromFiles.class.getName());

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);

        // STEP 1: create a SparkConf object
    //    if (args.length < 1) {
    //        log.fatal("Syntax Error: there must be one argument (a file name or a directory)");
    //        throw new RuntimeException();
    //    }

        // STEP 2: create a SparkConf object
        SparkConf sparkConf = new SparkConf()
                .setAppName("Spark Word count")
                .setMaster("local[4]");

        // STEP 3: create a Java Spark context
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sparkContext
                .textFile("data/quijote.txt")
                .persist(StorageLevel.MEMORY_ONLY());
        
        int numberOfLines = (int) lines.count();
        System.out.println("The number of lines of the data file is: " + numberOfLines);

        JavaRDD<String> words = lines.flatMap(line -> List.of(line.split(" ")).iterator());

        List<Tuple2<Integer, String>> results = sparkContext
                .textFile("data/quijote.txt")
                .flatMap(line -> List.of(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word,1))
                .reduceByKey((integer1, integer2) -> integer1 + integer2)
                .mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()))
                .sortByKey(false)
                .take(10);

        long finishTime = System.currentTimeMillis();

        // STEP 9: print the results
        for (Tuple2<?, ?> tuple : results) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }

        // STEP 19: stop the spark context
        sparkContext.stop();
    }
}
