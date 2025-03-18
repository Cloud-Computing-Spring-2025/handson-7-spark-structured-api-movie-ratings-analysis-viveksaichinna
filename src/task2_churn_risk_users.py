from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def analyze_churn_risk(df, spark_session):
    # Filter for users with canceled subscriptions and low watch time
    churn_risk = df.filter((col("SubscriptionStatus") == "Canceled") & (col("WatchTime") < 100))
    
    # Count unique users that match these criteria
    churn_risk_count = churn_risk.select("UserID").distinct().count()
    
    # Get total unique users for reference
    total_users = df.select("UserID").distinct().count()
    
    # Create a DataFrame with the results
    schema = StructType([
        StructField("Churn Risk Users", StringType(), False),
        StructField("Total Users", IntegerType(), False)
    ])
    
    result_df = spark_session.createDataFrame([
        ("Users with low watch time & canceled subscriptions", churn_risk_count)
    ], schema=schema)
    
    return result_df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("ChurnRiskAnalysis").getOrCreate()
    file_path = "input/movie_ratings_data.csv"
    
    # Read data
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
    
    # Ensure dataset is not empty
    if df.count() == 0:
        print("❌ ERROR: The dataset is empty. Please check 'input/movie_ratings_data.csv'.")
        spark.stop()
        exit()
    
    # Analyze churn risk
    result_df = analyze_churn_risk(df, spark)
    
    # Ensure the output directory exists
    import os
    output_dir = "outputs"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Save results to the correct directory
    output_path = os.path.join(output_dir, "churn_risk_users.csv")
    result_df.write.mode("overwrite").option("header", "true").csv(output_path)
    
    # Show results
    result_df.show()