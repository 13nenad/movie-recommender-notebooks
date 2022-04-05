// Databricks notebook source
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

import scalaj.http._
import scala.util.matching.Regex

import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.config.Config

val endPoint = "https://movie-recommender.documents.azure.com:443/"
val masterKey = "lT1muP4WJaoH2nGMaGgWXmfzKuCCmfwHSenILTvQNv9iIpbgEQDSlogkFNMD8gfqPzXsHRqLCDjljwNxTpPqTQ=="
val dbName = "movie-recommender"
val moviesCollectionName = "movies"
val ratingsCollectionName = "ratings"

// COMMAND ----------

// MAGIC %md ### Load Movie Dataset to CosmosDB

// COMMAND ----------

val moviesDS = spark.read
         .format("csv")
         .option("header", "true")
         .load("/FileStore/tables/movies.csv").distinct()

val moviesDF = moviesDS.toDF("id", "title", "genres")

val linksDS = spark.read
         .format("csv")
         .option("header", "true")
         .load("/FileStore/tables/links.csv").distinct()

val linksDF = linksDS.toDF("id", "imdbid", "tmdbId")

val joinLinksToMoviesDF = moviesDF.join(linksDF, "id").select("id", "title", "genres", "imdbid")
display(joinLinksToMoviesDF)

// COMMAND ----------

// Extract and add the release year from the title
var addReleasedYearDF = joinLinksToMoviesDF.withColumn("released", expr("substring(title, length(title)-4, 4)"))
addReleasedYearDF = addReleasedYearDF.withColumn("released", $"released".cast("int"))

// Remove the year from the title
addReleasedYearDF = addReleasedYearDF.withColumn("title", expr("substring(title, 0, length(title)-7)"))
display(addReleasedYearDF)

// COMMAND ----------

// Retrieve the poster jpg url for each imdb id - this will take a couple of hours depending on how many workers you have

val MovieLensUrl = "https://www.imdb.com/title/tt";
def ScrapePoster(imdbid: String): String = {
  
  val posterResp: HttpResponse[String] = Http(MovieLensUrl.concat(imdbid).concat("/")).asString

  val pattern: Regex = "https://m.media-amazon.com/images/M([a-zA-Z0-9:/'@._,-]+)".r

  val myMatch = pattern.findFirstMatchIn(posterResp.body).map(_.group(0))
  if (myMatch == null || myMatch == None) 
   return "Unknown"
  else 
    return myMatch.get
}

val PosterScraperUDF = udf(ScrapePoster _)
var addPosterLinkDF = addReleasedYearDF.withColumn("posterLink", PosterScraperUDF($"imdbid"))
addPosterLinkDF = addPosterLinkDF.select("id", "title", "genres", "released", "posterLink")

// COMMAND ----------

//addPosterLinkDF.write.format("csv").option("header", "true").save("dbfs:/FileStore/tables/MoviesWithPosterLinks.csv")
//val df = spark.read.format("csv").option("header", "true").load("/FileStore/tables/MoviesWithPosterLinks.csv")

// COMMAND ----------

val movieConfig = Config(Map("Endpoint" -> endPoint, "Masterkey" -> masterKey, "Database" -> dbName, "Collection" -> moviesCollectionName))

// Write movies to Cosmos DB
addPosterLinkDF.write.mode(SaveMode.Overwrite).cosmosDB(movieConfig)

// COMMAND ----------

// MAGIC %md ### Load Ratings Dataset to CosmosDB

// COMMAND ----------

val ratingsSchema = StructType(
  List(
    StructField("userId", IntegerType, nullable=false),
      StructField("movieId", IntegerType, nullable=false),
      StructField("rating", DoubleType, nullable=false),
      StructField("timestamp", LongType, nullable=false)
  )
)
  
val ratingsDS = spark.read
  .format("csv")
  .schema(ratingsSchema)  
  .option("header", "true")
  .load("/FileStore/tables/ratings.csv").select("userId", "movieId", "rating").distinct()

// this will be the collection's primary key and how we will find a user's rating
val addPrimaryKeyDF = ratingsDS.withColumn("id", concat($"userId", lit("-"), $"movieId")) 
display(addPrimaryKeyDF)

// COMMAND ----------

val ratingsConfig = Config(Map("Endpoint" -> endPoint, "Masterkey" -> masterKey, "Database" -> dbName, "Collection" -> ratingsCollectionName))

// Write ratings to Cosmos DB
addPrimaryKeyDF.write.mode(SaveMode.Overwrite).cosmosDB(ratingsConfig)
