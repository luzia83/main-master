from pyspark.sql import SparkSession, functions
from pyspark.sql.types import StructField, StringType, StructType


def main() -> None:
    spark_session = SparkSession\
        .builder\
        .appName("Analysis of tweets (Dataframe)")\
        .master("local[4]")\
        .getOrCreate()

    logger = spark_session._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    fields = [StructField("id", StringType(), True),
              StructField("user", StringType(), True),
              StructField("tweet", StringType(), True),
              StructField("dateTime", StringType(), True)]

    data_frame = spark_session \
        .read \
        .schema(StructType(fields)) \
        .load("data/tweets.tsv", format="csv", sep="\t")

    data_frame.printSchema()
    data_frame.show()

    new_lines = data_frame.rdd.cache()

    invalid_words = ("the", "in", "to", "at", "RT", "with", "a", "for", "of", "on", "")

    # Step 4. Compact most repeated word counts
    hit_words = new_lines \
        .map(lambda line: line[2]) \
        .flatMap(lambda line: line.split(" ")) \
        .map(lambda word: ''.join(filter(str.isalnum, word))) \
        .filter(lambda word: word not in invalid_words) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda pair: pair[1], ascending=False) \
        .take(10)

    # Step 5. Compact most active user
    user_result = new_lines \
        .map(lambda line: line[1]) \
        .map(lambda user: (user, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda pair: pair[1], ascending=False) \
        .first()

    # Step 6. Compact shortest tweet
    shortest = new_lines \
        .map(lambda line: (len(line[2]), line)) \
        .sortByKey(ascending=True) \
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