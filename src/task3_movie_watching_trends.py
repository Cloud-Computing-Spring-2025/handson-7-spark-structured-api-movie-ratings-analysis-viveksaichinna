from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def analyze_movie_trends(df):
    # Group by WatchedYear and count the number of movies watched per year
    movie_trends = df.groupBy("WatchedYear").agg(count("MovieID").alias("MoviesWatched"))
    
    # Order the results by year to visualize trends chronologically
    movie_trends = movie_trends.orderBy("WatchedYear")
    
    # Rename columns to match expected output
    movie_trends = movie_trends.select(
        col("WatchedYear").alias("Watched Year"),
        col("MoviesWatched").alias("Movies Watched")
    )
    
    return movie_trends

if __name__ == "__main__":
    spark = SparkSession.builder.appName("MovieWatchingTrends").getOrCreate()
    file_path = "input/movie_ratings_data.csv"
    
    # Read data
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
    
    # Ensure dataset is not empty
    if df.count() == 0:
        print("❌ ERROR: The dataset is empty. Please check 'input/movie_ratings_data.csv'.")
        spark.stop()
        exit()
    
    # Analyze movie watching trends
    result_df = analyze_movie_trends(df)
    
    # Ensure the output directory exists
    import os
    output_dir = "outputs"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Save results to the correct directory
    output_path = os.path.join(output_dir, "movie_watching_trends.csv")
    result_df.write.mode("overwrite").option("header", "true").csv(output_path)
    
    # Show results
    result_df.show()