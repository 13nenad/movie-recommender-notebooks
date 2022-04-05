# MovieRecommender Notebooks

Notebook implementation of a movie recommender engine used in a DataBricks cluster. An Alternating Least Squares machine learning algrithm was used which is a Collaborative Filtering technique.

## Collaborative Filtering
- A method of making automated predictions (filtering) about the interests of a user by collecting preference information from many users (collaborating)
- It takes the assumption that if a person A has the same opinion as a person B on a topic or item, A is more likely to have B's opinion on a different item than that of a randomly chosen person

## Data
- Initial data was obtained from https://grouplens.org/datasets/movielens/ 
- The small dataset was used which contains: 100,000 ratings applied to 9,000 movies by 600 users. Last updated 9/2018.
- Uploaded onto the cluster as CSV files in the DBFS

## Cluster Specs
- Spark 2.4.5
- Scala 2.11
- Libraries installed on the cluster:
  - azure_cosmosdb_spark_2_4_0_2_11_3_7_0_uber.jar (CosmosDB Spark Connector)
  - org.scalaj:scalaj-http_2.11:2.4.2

## Notebooks
LoadDataToCosmos - transforms and loads movie and rating data into CosmosDB, logic includes:
- Splitting out the year from the title e.g. "Class Action (2017)" -> "Class Action", "2017"
- Dataset provides imdb and tmdb IDs which helps find the movie in their respective websites e.g. https://www.imdb.com/title/tt0101590/ and https://www.themoviedb.org/movie/15771. Hence, I have logic to "scrape" the imdb website and obtain the movie "poster links" (jpg url)
- Creating a unique primary key for the ratings table by concatenating userId and movieId
- Saves data to Cosmos in the "movies" and "ratings" containers
  
### TuningALSParameters - hyper-parameter tuning
1. Split Rating data, training/test sets
2. Create parameterless ALS model
    - Specify user, item and rating columns
3. Build a parameter grid
    - I chose to tune 3 main parameters, with 5 values each -> 125 parameter combinations
5. Build a RegressionEvaluator and choose an evaluation metric
    - Root Mean Square Error (RMSE)
7. Build a CrossValidator, choose the number of folds
    - Random splits of training data (2/3) and validation data (1/3)
8. Build models using the training data
    - Try all ossible combinations
    - I chose to use 3 folds, that is 125 (5x5x5) * 3 = 375 models
9. Take the best model and make predictions on the test set
    - Evaluate the RMSE, if happy save the parameters in a file

### UpdateAllPredictedRatings - create or update predicted ratings for all users
1. Extract current ratings from Cosmos
2. Create an ALS model using the best parameters obtained in TuningALSParameters notebook
3. Use the model on the ratings data to produce "predicted ratings" for all movies for every user
    - The webapp has the rating bound between 0.5 - 5.0, hence I capped ratings outside of this range
4. Save data to Cosmos in the "predicted-ratings" container

**Note:** I could have made the model only produce the top K predicted ratings (recommendations) which would save a lot of space in the DB, but I chose to display the predicted rating for the user in focus on the "movie details" screen, hence I chose to save all predicted ratings

**Note:** In "production" I would have this logic run daily, so the users can have more accurate recommendations

### UpdatePredictedRatingsForUser
- Same as above but for demonstration purposes it only updates a specified user's predicted ratings
- The specified user is defined through a notebook input parameter
- This notebook is attached to a job which is triggered by the webapp

### UpdateRatingAverages
1. Grab all of the ratings from Cosmos
2. Calculate the average rating for every movie
3. Grab all of the movie details data
4. Drop the old average ratings column and join the new one
5. Save/upload new movie details data

**Note:** In "production" I would have this logic run daily, so the users have an accurate average rating representation

### ConstantsAndUtils
- Purpose of this notebook is to have code reusability
- Contains string constants often used in the above notebooks
- Contains often used logic which has been broken up into separate functions
