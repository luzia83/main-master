from pyspark import SparkConf, SparkContext


def main() -> None:

    spark_conf = SparkConf().setAppName("Film locations in San Francisco").setMaster("local[4]")
    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    # Step 3. Read the file contents
    lines = spark_context.textFile("data/Film_Locations_in_San_Francisco.csv")

    # Step 4. Read the fields
    line_data = lines.map(lambda line: line.split(';'))
    all_films_names = line_data.map(lambda row : row[0])
    number_films = all_films_names\
        .distinct()\
        .count()

    # Step 5. Filter rows to avoid execution time errors on some wrong lines
    lineDataFiltered = line_data.filter(lambda line : len(line) > 3);

    # Step 6: map operation to isolate name and location fields
    filmNameLocation = lineDataFiltered\
        .map(lambda row : (row[0], row[2]))

    # Step 7. Just consider distinct pairs name, location
    filmNameLocationDistinct = filmNameLocation.distinct()

    # Step 8. Take the name of the film
    filmName = filmNameLocationDistinct.map(lambda row : row[0])

    # STEP 9: map operation to create pairs <type, 1> per every row
    pairs = filmName\
        .map(lambda row : (row, 1))

    # Step 10. Reduce operation that sum the values of all the pairs having the same key, generating a pair <key, sum>
    grouped_pairs = pairs\
        .reduceByKey(lambda integer, integer2 : integer + integer2)

    number_locations = grouped_pairs.map(lambda row : row[1])

    total_locations = number_locations.reduce(lambda a, b: a + b)

    avg_locations = total_locations / number_films

    # Step 11. Apply a filter to get those films with at least 20 locations
    pairs_results = grouped_pairs\
        .filter(lambda row: row[1] >= 20)

    # Step 12. Order pairs by count
    ordered_pairs = pairs_results.sortBy(lambda pair: pair[1], ascending=False)

    result = ordered_pairs.collect()

    # Step 13: print the results and save to a text file
    with open("film_locationsSanFrancisco.txt", 'w') as output_file:
        for (value, count) in result:
            print("(%i, %s)" % (count, value))
            output_file.writelines("(%i, %s)\n" % (count, value))
        line1 = "Total number of films: {nf}\n".format(nf=str(number_films))
        line2 = "The average of film locations per film: {al}\n".format(al=str(avg_locations))
        print(line1, line2)
        output_file.write(line1)
        output_file.write(line2)


if __name__ == '__main__':
    main()