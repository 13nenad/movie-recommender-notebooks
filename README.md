## MovieRecommender Notebooks

Notebook implementation of a movie recommender engine used in a DataBricks cluster. An Alternating Least Squares machine learning algrithm was used which is a Collaborative Filtering technique.

# Collaborative Filtering
- A method of making automated predictions (filtering) about the interests of a user by collecting preference information from many users (collaborating)
- It takes the assumption that if a person A has the same opinion as a person B on a topic or item, A is more likely to have B's opinion on a different item than that of a randomly chosen person

# Data
- Initial data was obtained from https://grouplens.org/datasets/movielens/ 
- The small dataset was used which contains: 100,000 ratings applied to 9,000 movies by 600 users. Last updated 9/2018.
- Uploaded onto the cluster as CSV files in the DBFS

# Cluster Specs
- Spark 2.4.5
- Scala 2.11
- Libraries installed on the cluster:
  - azure_cosmosdb_spark_2_4_0_2_11_3_7_0_uber.jar (CosmosDB Spark Connector)
  - org.scalaj:scalaj-http_2.11:2.4.2

# Notebooks
LoadDataToCosmos - transforms and loads movie and rating data into CosmosDB, logic includes:
- Splitting out the year from the title e.g. "Class Action (2017)" -> "Class Action", "2017"
- Dataset provides imdb and tmdb IDs which helps find the movie in their respective websites e.g. https://www.imdb.com/title/tt0101590/ and https://www.themoviedb.org/movie/15771. Hence, I have logic to "scrape" the imdb website and obtain the movie "poster links" (jpg url)
- Creating a unique primary key for the ratings table by concatenating userId and movieId
  
TuningALSParameters - hyper-parameter tuning
1. Split Rating data, training/test sets
2. Create parameterless ALS model
  - Specify user, item and rating columns
3. Build a parameter grid
  - I chose to tune 3 main parameters, with 5 values each -> 125 parameter combinations
4. Build a RegressionEvaluator and choose an evaluation metric - Root Mean Square Error (RMSE)
5. Build a CrossValidator, choose the number of folds
  - Random splits of training data (2/3) and validation data (1/3)
6. Build models using the training data
  - Try all ossible combinations
  - I chose to use 3 folds, that is 125 (5x5x5) * 3 = 375 models
7. Take the best model and make predictions on the test set
  - Evaluate the RMSE, if happy save the parameters in a file
