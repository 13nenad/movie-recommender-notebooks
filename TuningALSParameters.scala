// Databricks notebook source
// MAGIC %run /Shared/MovieRecommender/ConstantsAndUtils

// COMMAND ----------

val ratingsDF = extractDataframeFromCosmosContainer(RATINGS_CONTAINER_NAME)
display(ratingsDF)

// COMMAND ----------

// Split ratings dataset into training (70%) and testing (30%)
val randSplitArr = ratingsDF.randomSplit(Array(0.7, 0.30), seed = 12345L)
var (trainingData, testingData) = (randSplitArr(0), randSplitArr(1))
println("Training: " + trainingData.count() + " Testing: " + testingData.count())

// COMMAND ----------

import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}

// Build a generic ALS model without hyperparameters
val model = new ALS()
  .setUserCol("userId")
  .setItemCol("movieId")
  .setRatingCol("rating")

// Set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
model.setColdStartStrategy("drop")

// Tell spark what values to try for each hyperparameter - I have chosen to try with 5 x 5 x 5 = 125 different parameter combinations
val paramGrid = new ParamGridBuilder()
  .addGrid(model.rank, Array(10, 20, 30, 40, 50))
  .addGrid(model.maxIter, Array(5, 10, 15, 20, 25))
  .addGrid(model.regParam, Array(0.1, 0.25, 0.5, 1, 1.5))
  .build()

// Tell spark how to evaluate model performance
val evaluator = new RegressionEvaluator()
  .setMetricName("rmse")
  .setPredictionCol("prediction")
  .setLabelCol("rating")

// Create a cross validator with our default model, regression evaluator and parameter grid
// CrossValidator will generate 3 (training, test) dataset pairs, each of which uses 2/3 of the data for training and 1/3 for validation
val cv = new CrossValidator()
  .setEstimator(model)
  .setEvaluator(evaluator)
  .setEstimatorParamMaps(paramGrid)
  .setNumFolds(3)
  .setParallelism(3) // Evaluate up to 3 parameter combinations in parallel

// COMMAND ----------

// Run the cross validator on the training data
val cvModel = cv.fit(trainingData)

// Extract the best combination of values from cross validation
val bestModel = cvModel.bestModel

// Generate a column of predictions which is added to the test data and then evaluate using RMSE
val predictions = bestModel.transform(testingData)
val rmse = evaluator.evaluate(predictions)

// Do a bit of cast trickery to be able to get the best hyper-parameters
val alsBestModel = bestModel.parent.asInstanceOf[ALS]

println("---Best Model---")
println("RMSE: " + rmse)
println("Rank: " + alsBestModel.getRank)
println("MaxIter: " + alsBestModel.getMaxIter)
println("RegParam: " + alsBestModel.getRegParam)

// COMMAND ----------

// Save best hyper-parameter values to a file in DBFS
val paramValues = Seq((alsBestModel.getRank, alsBestModel.getMaxIter, alsBestModel.getRegParam))
val alsModelRDD = spark.sparkContext.parallelize(paramValues)
val alsModelDF = alsModelRDD.toDF("Rank", "MaxIter", "RegParam")
display(alsModelDF)
alsModelDF.write.mode("overwrite").option("header", "true").format("csv").save(BEST_PARAMS_FILE_PATH)

// COMMAND ----------

// Make sure the parameters are saved correctly
spark.read.option("header", "true").csv(BEST_PARAMS_FILE_PATH).show()
