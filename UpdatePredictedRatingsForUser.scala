// Databricks notebook source
// MAGIC %run /Shared/MovieRecommender/ConstantsAndUtils

// COMMAND ----------

val ratingsDF = extractDataframeFromCosmosContainer(RATINGS_CONTAINER_NAME)
display(ratingsDF)

// COMMAND ----------

/* Build an ALS user matrix model based on the tuned hyper-parameters we found in TuningALSParameters notebook. This technique predicts missing ratings for specific users and specific movies based on ratings for those movies from other users who gave similar ratings for other movies
Parameters:
  Rank: number of features to use (also referred to as the number of latent factors)
  MaxIter: number of iterations of ALS to run, ALS typically converges to a reasonable solution in 20 iterations or less
  RegParam: specifies the regularization parameter in ALS (defaults to 1.0).
*/

import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.functions._

val bestParamsDF = spark.read.option("header", "true").csv(BEST_PARAMS_FILE_PATH)
display(bestParamsDF)

val alsModel = new ALS()
  .setRank(bestParamsDF.select(col("Rank")).first.getString(0).toInt) 
  .setMaxIter(bestParamsDF.select(col("MaxIter")).first.getString(0).toInt)
  .setRegParam(bestParamsDF.select(col("RegParam")).first.getString(0).toDouble)
  .setUserCol("userId")
  .setItemCol("movieId")
  .setRatingCol("rating")
  .fit(ratingsDF)

// COMMAND ----------

// Get a predicted rating for every movie for every user
var predictedUserRatings = predictedRatingsForAllUsersOfAllMovies(alsModel, ratingsDF)

// Only get all the predicted ratings for a specified user
val userId = dbutils.widgets.get("userId")
val predictedRatingsForUser = predictedUserRatings.select("*").where($"id" === userId)
display(predictedRatingsForUser)

// COMMAND ----------

saveDataFrameToCosmosContainer(PREDICTIONS_CONTAINER_NAME, predictedUserRatings)
