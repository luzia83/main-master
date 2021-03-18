package org.mbd.m9;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Example implementing the Spark version of the "Hello World" Big Data program: counting the
 * number of occurrences of words in a set of files
 *
 * @author Antonio J. Nebro <antonio@lcc.uma.es>
 */

public class WordCountFromFiles {
    static Logger log = Logger.getLogger(WordCountFromFiles.class.getName());

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);

        // STEP 1: create a SparkConf object
        //if (args.length < 1) {
        //    log.fatal("Syntax Error: there must be one argument (a file name or a directory)");
        //    throw new RuntimeException();
        //}

        // STEP 2: create a SparkConf object
        SparkConf sparkConf = new SparkConf()
                .setAppName("Spark Word count")
                .setMaster("local[4]");

        // STEP 3: create a Java Spark context
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // STEP 4: read lines of files
        //JavaRDD<String> lines = sparkContext.textFile(args[0]);
        JavaRDD<String> lines = sparkContext.textFile("data/quijote.txt");

        // STEP 5: split the lines into words
        JavaRDD<String> words = lines
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        // STEP 6: map operation to create pairs <word, 1> per every word
        JavaPairRDD<String, Integer> pairs = words
                .mapToPair(word -> new Tuple2<>(word, 1));

        // STEP 6: reduce operation that sum the values of all the pairs having the same key (word),
        //         generating a pair <key, sum>
        JavaPairRDD<String, Integer> groupedPairs = pairs
                .reduceByKey((integer, integer2) -> integer + integer2);

        // STEP 7: map operation to get an RDD of pairs <sum, key>. We need this step because Spark
        //         Spark provides a sortByKey() funcion (see next step) but not a sortByValue()
        JavaPairRDD<Integer, String> reversePairs = groupedPairs
                .mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()));

        // STEP 8: sort the results by key ant take the first 20 elements
        List<Tuple2<Integer, String>> output = reversePairs
                .sortByKey(false)
                .take(20);

        // STEP 9: print the results
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }

        // STEP 19: stop the spark context
        sparkContext.stop();
    }
}
