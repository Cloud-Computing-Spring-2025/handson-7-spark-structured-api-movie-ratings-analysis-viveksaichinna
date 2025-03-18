from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round

def analyze_binge_watching(df):
    # Count total distinct users in each age group
    total_users_by_age = (
        df.select("UserID", "AgeGroup")
        .dropDuplicates()
        .groupBy("AgeGroup")
        .count()
        .withColumnRenamed("count", "TotalUsers")
    )

    # Count binge watchers in each age group
    binge_watchers = (
        df.filter(col("IsBingeWatched") == True)
        .select("UserID", "AgeGroup")
        .dropDuplicates()
        .groupBy("AgeGroup")
        .count()
        .withColumnRenamed("count", "BingeWatchers")
    )

    # Join the two DataFrames and calculate percentages
    result = binge_watchers.join(total_users_by_age, "AgeGroup")
    result = result.withColumn("Percentage", spark_round((col("BingeWatchers") / col("TotalUsers") * 100), 2))

    # Select and order columns for final output
    result = result.select("AgeGroup", "BingeWatchers", "Percentage")
    return result

if __name__ == "__main__":
    spark = SparkSession.builder.appName("BingeWatchingAnalysis").getOrCreate()
    file_path = "input/movie_ratings_data.csv"

    # Read data
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)

    # Ensure dataset is not empty
    if df.count() == 0:
        print("❌ ERROR: The dataset is empty. Please check 'input/movie_ratings_data.csv'.")
        spark.stop()
        exit()
    
    # Analyze binge-watching patterns
    result_df = analyze_binge_watching(df)

    # Ensure the output directory exists
    import os
    output_dir = "outputs"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Save results to the correct directory
    output_path = os.path.join(output_dir, "binge_watching_patterns.csv")
    result_df.write.mode("overwrite").option("header", "true").csv(output_path)

    # Show results
    result_df.show()