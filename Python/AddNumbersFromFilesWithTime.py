import sys
import time

from pyspark import SparkConf, SparkContext


def main(file_name: str) -> None:
    spark_conf = SparkConf()\
        .setAppName("AddNumbers")\
        .setMaster("local[8]")

    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    start_computing_time = time.time()

    sum = spark_context \
        .textFile(file_name) \
        .map(lambda line: int(line)) \
        .reduce(lambda x, y: x + y)

    total_computing_time = time.time() - start_computing_time

    print("Sum: ", sum)
    print("Computing time: ", str(total_computing_time))

    spark_context.stop()


if __name__ == "__main__":
    """
    Python program that uses Apache Spark to sum a list of numbers stored in files
    """


    main("data/manyNumbers.txt")


