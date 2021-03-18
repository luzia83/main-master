import time

from pyspark import SparkConf, SparkContext


def main() -> None:

    spark_conf = SparkConf().setAppName("WordCount").setMaster("local[2]")
    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    init_time = time.time()
    stop_words = ("que", "en", "de", "y", "la", "el")
    #stopWords = set(stopwords.words('spanish'))
    print("Number of stop words: ", len(stop_words))

    output = spark_context.textFile("data/quijote.txt")\
        .flatMap(lambda line: line.split(' ')) \
        .filter(lambda word: word not in stop_words)\
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda pair: pair[1], ascending=False) \
        .take(20)

    finish_time = time.time()

    for (word, count) in output:
        print("%s: %i" % (word, count))

    print("Computing time: " + str(finish_time - init_time))
    spark_context.stop()


if __name__ == '__main__':
    main()