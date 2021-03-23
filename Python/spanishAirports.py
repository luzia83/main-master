from pyspark import SparkConf, SparkContext


def main() -> None:

    spark_conf = SparkConf().setAppName("Spanish airports").setMaster("local[4]")
    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    lines = spark_context.textFile("data/airports.csv")

    line_data = lines.map(lambda line: line.split(','))

    line_data_es = line_data.filter(lambda row: row[8] == "\"ES\"")

    # Step 6. Take only field 'type'
    airport_types = line_data_es.map(lambda row: row[2])

    # STEP 7: map operation to create pairs <type, 1> per every row
    pairs = airport_types.map(lambda row: (row, 1))

    grouped_pairs = pairs.reduceByKey(lambda a, b: a + b)

    ordered_pairs = grouped_pairs.sortBy(lambda pair: pair[1], ascending=False)

    result = ordered_pairs.collect()

    with open("spanishAirports.txt", 'w') as output_file:
        for (value, count) in result:
            print("%s: %i" % (value, count))
            output_file.writelines("%s: %i\n" % (value, count))


if __name__ == '__main__':
    main()