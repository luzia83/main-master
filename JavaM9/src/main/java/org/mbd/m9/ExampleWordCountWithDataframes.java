package org.mbd.m9;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import org.apache.spark.sql.functions.*;

import static org.apache.spark.sql.functions.*;

public class ExampleWordCountWithDataframes {

  public static void main(String[] args) {

    if (args.length < 1) {
      System.err.println("Usage: ExampleWordCountWithDataframes <file> ");
      System.exit(1);
    }

    Logger.getLogger("org").setLevel(Level.OFF);

    SparkSession sparkSession = SparkSession.builder().master("local[4]").getOrCreate();

    Dataset<String> dataFrame =
        sparkSession
            .read()
            .format("text")
            .option("header", "false")
            .load(args[0])
            .as(Encoders.STRING());

    dataFrame.printSchema();
    dataFrame.show();

    Dataset<String> words =
        dataFrame
            .flatMap(
                (FlatMapFunction<String, String>) line -> Arrays.asList(line.split(" ")).iterator(),
                Encoders.STRING());

    words
        .groupBy("value")
        .count()
        .orderBy(col("count").desc())
        .show();
  }
}
