### MovieRecommender Notebooks

Notebook implementation of a movie recommender engine used in a DataBricks cluster. An Alternating Least Squares machine learning algrithm was used which is a Collaborative Filtering technique.

Collaborative Filtering
- A method of making automated predictions (filtering) about the interests of a user by collecting preference information from many users (collaborating)
- It takes the assumption that if a person A has the same opinion as a person B on a topic or item, A is more likely to have B's opinion on a different item than that of a randomly chosen person

Data
- Initial data was obtained from https://grouplens.org/datasets/movielens/ 
- The small dataset was used which contains: 100,000 ratings applied to 9,000 movies by 600 users. Last updated 9/2018.
- Uploaded onto the cluster as CSV files in the DBFS

Cluster used
- Spark 2.4.5
- Scala 2.11
- Libraries installed on the cluster:
  - azure_cosmosdb_spark_2_4_0_2_11_3_7_0_uber.jar (CosmosDB Spark Connector)
  - org.scalaj:scalaj-http_2.11:2.4.2
