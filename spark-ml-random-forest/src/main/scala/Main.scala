import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, StringIndexerModel, VectorAssembler, VectorIndexer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local[*]").appName("Predict MOT result").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val filesPath = "/home/user/download/dft_test_result_2020"
    val motData = spark.read.option("header", "true").csv(filesPath)
    motData
      .withColumn("year", year('first_use_date))
      .repartition('year, 'make, 'model)
      .write
      .format("parquet")
      .saveAsTable("/home/user/motdata")

    val _data =
          spark.read
            .parquet("/home/user/motdata")
            .filter(
            """test_type = 'NT'
                | and test_result in ('F', 'P')
                |""".stripMargin)

    val data = _data
      .drop("colour", "vehicle_id", "test_id", "test_date", "test_class_id", "test_type", "postcode_area", "first_use_date")
      .withColumn("indexed_test_mileage", expr("int(test_mileage)"))
      .withColumn("indexed_year", expr("int(year)"))
      .withColumn("indexed_cylinder_capacity", expr("int(cylinder_capacity)"))
      .filter("make = 'FORD'")
      .filter("model = 'FIESTA'")

    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol("test_result")
      .setOutputCol("indexedTestResult")
      .fit(data)

    val stringCols = List("make", "model", "fuel_type")
    val featureCols = List("test_mileage", "fuel_type", "year", "cylinder_capacity")

    def getStringIndexer(col: String): StringIndexer = {
      new StringIndexer()
        .setInputCol(col)
        .setOutputCol(s"indexed_$col")
    }

    val nullsFiltered =
      featureCols
        .foldLeft(data){case (df, c)=> df.filter(s"$c is not null")}
        .localCheckpoint(true)

    val convertedStringColsDf =
      stringCols
        .map(getStringIndexer)
        .foldLeft(nullsFiltered){case(df, i)=>i.fit(df).transform(df)}

    val featuresAssembler = new VectorAssembler()
      .setInputCols(featureCols.map(c=>s"indexed_$c").toArray)
      .setOutputCol("features")

    val withFeaturesVectorDf =
      featuresAssembler.
        transform(convertedStringColsDf)
        .drop(featureCols.map(c=>s"indexed_$c"): _*)
        .localCheckpoint(true)

    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(withFeaturesVectorDf)

    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = withFeaturesVectorDf.randomSplit(Array(0.7, 0.3))

    // Train a RandomForest model.
    val rf = new RandomForestClassifier()
      .setLabelCol("indexedTestResult")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(30)
      .setMaxBins(50)

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedTestResult")
      .setLabels(labelIndexer.labelsArray(0))

    // Chain indexers and forest in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

    val USE_SAVED_MODEL = false
    val path = "/home/user/models/spark-rf/model"
    // Train model. This also runs the indexers.
    val model = if (USE_SAVED_MODEL) {
      PipelineModel.load(path)
    }else {
      val m = pipeline.fit(trainingData)
      m.write.overwrite().save(path)
      m
    }

    // Make predictions.
    val predictions = model.transform(testData)

    // Select example rows to display.
    val cols = List("predictedTestResult", "test_result") ++ featureCols
    // predictions.selectExpr(cols: _*).show(1500)
    predictions
      .repartition(1)
      .selectExpr(cols: _*)
      .write
      .mode("overwrite")
      .csv("/tmp/output.csv")

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedTestResult")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println(s"Test Accuracy = $accuracy")
    println(s"Test Error = ${(1.0 - accuracy)}")
  }
}