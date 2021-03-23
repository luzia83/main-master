from pyspark import SparkConf, SparkContext

def main() -> None:

    # Step 1. Create a SparkConf object, takes 4 cores and a Java Spark context
    spark_conf = SparkConf().setAppName("Analysis of tweets (RDD)").setMaster("local[4]")
    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    # Step 2. Create an invalid words list
    invalid_words = ("the","in","to","at","RT","with","a","for","of","on","")

    # Step 3. Read the file contents
    lines = spark_context.textFile("data/tweets.tsv")

    # Step 4. Compact most repeated word counts
    hit_words = lines\
                .map(lambda line: line.split("\t"))\
                .map(lambda line : line[2])\
                .flatMap(lambda line : line.split(" "))\
                .map(lambda word : ''.join(filter(str.isalnum, word)))\
                .filter(lambda word : word not in invalid_words)\
                .map(lambda word : (word, 1))\
                .reduceByKey(lambda a, b : a + b )\
                .sortBy(lambda pair: pair[1], ascending=False)\
                .take(10)

    # Step 5. Compact most active user
    user_result = lines\
        .map(lambda line : line.split("\t"))\
        .map(lambda line : line[1])\
        .map(lambda user : (user, 1))\
        .reduceByKey(lambda a, b : a + b)\
        .sortBy(lambda pair: pair[1], ascending=False)\
        .first()

    # Step 6. Compact shortest tweet
    shortest = lines\
        .map(lambda line : line.split("\t"))\
        .map(lambda line : (len(line[2]), line))\
        .sortByKey(ascending=True)\
        .first()

    # Step 7. Print out the solutions
    print("10 MOST REPEATED WORDS: ")
    for (word, count) in hit_words:
        print(word, ": ", count)
    print("MOST ACTIVE USER: ", user_result[0], " with ", user_result[1], " tweets")
    print("SHORTEST TWEET: ", shortest[1][2], " (", shortest[0], " char). User: ", shortest[1][1], ". Date and time: ",
          shortest[1][3])


if __name__ == '__main__':
    main()