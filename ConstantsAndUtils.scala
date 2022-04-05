// Databricks notebook source
val COSMOS_ENDPOINT = "https://movie-recommender.documents.azure.com:443/"
val MASTER_KEY = "lT1muP4WJaoH2nGMaGgWXmfzKuCCmfwHSenILTvQNv9iIpbgEQDSlogkFNMD8gfqPzXsHRqLCDjljwNxTpPqTQ=="
val DB_NAME = "movie-recommender"
val MOVIES_CONTAINER_NAME = "movies"
val RATINGS_CONTAINER_NAME = "ratings"
val PREDICTIONS_CONTAINER_NAME = "predicted-ratings"
val BEST_PARAMS_FILE_PATH = "/FileStore/AlsModelParams"

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import com.microsoft.azure.cosmosdb.spark.config.Config
import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.schema._

def extractDataframeFromCosmosContainer(containerName:String) : DataFrame = {
  
    val config = Config(Map("Endpoint" -> COSMOS_ENDPOINT, 
                            "Masterkey" -> MASTER_KEY, 
                            "Database" -> DB_NAME, 
                            "Collection" -> containerName))
  
    return spark.read.cosmosDB(config)
  
}

// COMMAND ----------

import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.functions._

def predictedRatingsForAllUsersOfAllMovies(alsModel:ALSModel, ratingsDF:DataFrame) : DataFrame = {

  // You could only get the top K movie recommendations for each user here, but I'm getting the "predicted ratings"
  // for ALL movies (hence why calling count()), so I can show the predicted rating when showing the "movie details page"
  var predictedUserRatings = alsModel.recommendForAllUsers(ratingsDF.select("movieId").distinct().count().toInt)
  
  predictedUserRatings = predictedUserRatings.select($"userId".cast("String") as "id", 
                                               $"recommendations.movieId" as "movieIds", 
                                               $"recommendations.rating" as "predictedRatings")
  
  var explodedUserRatings = predictedUserRatings.select($"id", explode($"predictedRatings") as "predictedRatings")
                                                 .select($"id", bround($"predictedRatings".cast("Double"), 1) as "predictedRatings")
  
  // Cap the predictions to 0.5 - 5.0
  explodedUserRatings = predictedUserRatings.withColumn("predictedRatings", when($"predictedRatings" > 5.0, 5.0).otherwise($"predictedRatings"))
  explodedUserRatings = predictedUserRatings.withColumn("predictedRatings", when($"predictedRatings" < 0.5, 0.5).otherwise($"predictedRatings"))
  
  explodedUserRatings = predictedUserRatings.groupBy($"id").agg(collect_list($"predictedRatings") as "predictedRatings")
  predictedUserRatings = predictedUserRatings.drop("predictedRatings").join(predictedUserRatings, "id")
  
  return predictedUserRatings
}

// COMMAND ----------

import org.apache.spark.sql.SaveMode

def saveDataFrameToCosmosContainer(containerName:String, dataFrameToSave:DataFrame) : Unit = {
  
    val config = Config(Map("Endpoint" -> COSMOS_ENDPOINT, 
                            "Masterkey" -> MASTER_KEY, 
                            "Database" -> DB_NAME, 
                            "Collection" -> containerName))
  
    dataFrameToSave.write.mode(SaveMode.Overwrite).cosmosDB(config)
  
}
