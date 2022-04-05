// Databricks notebook source
// MAGIC %run /Shared/MovieRecommender/ConstantsAndUtils

// COMMAND ----------

val ratingsDF = extractDataframeFromCosmosContainer(RATINGS_CONTAINER_NAME)
display(ratingsDF)

// COMMAND ----------

// Calculate the average rating for each movie
val avgRatingDF = ratingsDF.groupBy($"movieId").agg(bround(avg($"rating"), 1).as("avgRating"))
display(avgRatingDF)

// COMMAND ----------

// Get the total number of ratings for each movieval ratingsDF = extractDataframeFromCosmosContainer(RATINGS_CONTAINER_NAME)
display(ratingsDF)
val numOfRatingsDF = ratingsDF.groupBy($"movieId").count().select($"movieId", $"count".cast("Integer").as("numOfRatings"))
display(numOfRatingsDF)

// COMMAND ----------

val moviesDF = extractDataframeFromCosmosContainer(MOVIES_CONTAINER_NAME)
display(moviesDF)
// id represents movieId, we let it be "id" so CosmosDB makes it the primary key

// COMMAND ----------

// Remove old average rating and add the new average rating to each movie
val moviesWithAvgRatingDF = moviesDF.drop("avgRating").join(avgRatingDF, $"id" === $"movieId").drop("movieId")

// Add the total number of ratings to each movie
val moviesWithNumOfRatingsDF = moviesWithAvgRatingDF.join(numOfRatingsDF, $"id" === $"movieId").drop("movieId")

display(moviesWithNumOfRatingsDF)

// COMMAND ----------

saveDataFrameToCosmosContainer(MOVIES_CONTAINER_NAME, moviesWithNumOfRatingsDF)
