from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark_session = SparkSession \
        .builder \
        .appName("Spark Random Forest with dataframes") \
        .master("local[4]") \
        .getOrCreate()

    data_frame = spark_session \
        .read \
        .format("libsvm") \
        .load("data/breast-cancer_scale.txt")

    data_frame.printSchema()
    data_frame.show()

    (training_data, test_data) = data_frame.randomSplit([0.75, 0.25])

    num_trees = 30

    random_forest = RandomForestClassifier()\
        .setNumTrees(num_trees)\
        .setLabelCol("label")\
        .setPredictionCol("prediction")

    rf_model = random_forest.fit(training_data)

    prediction = rf_model.transform(test_data)
    prediction.printSchema()

    prediction\
        .select("prediction", "label", "features")\
        .show(5)

    # Select (prediction, true label) and compute test error
    evaluator = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(prediction)
    print("Test Error = %g " % (1.0 - accuracy))

    spark_session.stop()
