package org.mbd.m9;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TweetsDF {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);

        // Step 1. Create a SparkSession object, takes 4 cores and a Java Spark context
        SparkSession sparkSession =
                SparkSession.builder()
                        .appName("Analysis of tweets (Dataframe)")
                        .master("local[4]")
                        .getOrCreate();

        StructType explicitSchema =
                new StructType()
                        .add("id", "string")
                        .add("user", "string")
                        .add("tweet", "string")
                        .add("dateTime", "string");

        // Step 2. Read the data on a dataframe with explicit schema
        Dataset<Row> dataFrame = sparkSession
                .read()
                .format("csv")
                .option("sep", "\t")
                .schema(explicitSchema)
                .load("data/tweets.tsv");

        dataFrame.printSchema();
        dataFrame.show();

        JavaRDD<Row> lines = dataFrame.toJavaRDD();
        JavaRDD<String> newLines = lines.map(line -> line.mkString("\t"));

        // Step 3. Create an invalid words list
        ArrayList<String> invalidWords = new ArrayList<String>();
        invalidWords.add("the");
        invalidWords.add("in");
        invalidWords.add("to");
        invalidWords.add("at");
        invalidWords.add("RT");
        invalidWords.add("with");
        invalidWords.add("a");
        invalidWords.add("for");
        invalidWords.add("of");
        invalidWords.add("on");
        invalidWords.add("");

        // Step 4. Compact most repeated word counts
        List<Tuple2<Integer, String>> hitWords = newLines
                .map(line -> line.split("\t"))
                .map(line -> line[2])
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .map(word -> word.replaceAll("[\\+\\-\\.#:,?!_]", ""))
                .filter(word -> !invalidWords.contains(word))
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum)
                .mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()))
                .sortByKey(false)
                .take(10);

        // Step 5. Compact most active user
        Tuple2<Integer, String> userResult = newLines
                .map(line -> line.split("\t"))
                .map(line -> line[1])
                .mapToPair(user -> new Tuple2<>(user, 1))
                .reduceByKey(Integer::sum)
                .mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()))
                .sortByKey(false)
                .first();

        // Step 6. Compact shortest tweet
        Tuple2<Integer, String[]> shortest = newLines
                .map(line -> line.split("\t"))
                .mapToPair(line -> new Tuple2<>(line[2].length(), line))
                .sortByKey(true)
                .first();

        // Step 7. Print out the solutions
        System.out.println("10 MOST REPEATED WORDS: ");
        for (Tuple2<?, ?> tuple : hitWords) {
            System.out.println(tuple._2() + ": " + tuple._1());
        }
        System.out.println("MOST ACTIVE USER: " + userResult._2() + " with " + userResult._1() + " tweets");
        System.out.println("SHORTEST TWEET: " + shortest._2()[2] + " (" + shortest._1() + " char). User: " + shortest._2()[1] +
                ". Date and time: " + shortest._2()[3]);

        sparkSession.stop();
    }
}
