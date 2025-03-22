import findspark
findspark.init()

import os
# Set the Spark version environment variable for PyDeequ
os.environ['SPARK_VERSION'] = '3.3'  # Adjust to match your actual Spark version

from pyspark.sql import SparkSession
from pydeequ.analyzers import *
from pydeequ.checks import *
from pydeequ.verification import *
from pyspark.sql.functions import lit, when

"""
Set up and Creation of the data using PySpark
"""
# Create Spark session with Deequ configuration
spark = SparkSession.builder \
    .appName("SparkDeequTest") \
    .master("local[*]") \
    .config("spark.jars.packages", "com.amazon.deequ:deequ:2.0.3-spark-3.3") \
    .getOrCreate()


spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
spark.conf.set("spark.sql.repl.eagerEval.truncate", 0)  

# Load data
movie_data = spark.read.csv("blog_data.csv", header=True, inferSchema=True)
movie_data_bad = spark.read.csv("blog_data_wrong.csv", header=True, inferSchema=True)

"""Checking for data Properties"""
core_movie_df = movie_data.select("user_id", "movie_id", "rating", "title", "Action", "Comedy", "Crime")
core_movie_df_bad = movie_data_bad.select("user_id", "movie_id", "rating", "title", "Action", "Comedy", "Crime")

print("Sample data:")
core_movie_df.show(5, truncate=0)  # Changed to integer 0
print(f"Total records: {core_movie_df.count()}")


"""
Set up and Creation of the data using PySpark
"""
# Create Spark session with Deequ configuration
spark = SparkSession.builder \
    .appName("SparkDeequTest") \
    .master("local[*]") \
    .config("spark.jars.packages", "com.amazon.deequ:deequ:2.0.3-spark-3.3") \
    .getOrCreate()

# Load data
movie_data = spark.read.csv("blog_data.csv", header=True, inferSchema=True)
movie_data_bad = spark.read.csv("blog_data_wrong.csv", header=True, inferSchema=True)

"""Checking for data Properties"""
core_movie_df = movie_data.select("user_id", "movie_id", "rating", "title", "Action", "Comedy", "Crime")
core_movie_df_bad = movie_data_bad.select("user_id", "movie_id", "rating", "title", "Action", "Comedy", "Crime")

print("Sample data:")
core_movie_df.show(5, truncate=0)
core_movie_df.printSchema()
print(f"Total records: {core_movie_df.count()}")

"""
Run analyzers for basic statistics
"""
print("\n=== DATA ANALYSIS RESULTS ===")

analysisResult = AnalysisRunner(spark) \
    .onData(core_movie_df) \
    .addAnalyzer(Size()) \
    .addAnalyzer(Mean("rating")) \
    .addAnalyzer(Minimum("rating")) \
    .addAnalyzer(Maximum("rating")) \
    .addAnalyzer(StandardDeviation("rating")) \
    .addAnalyzer(ApproxCountDistinct("movie_id")) \
    .run()

# Display analyzer results in a clean format
analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)
analysisResult_df.show(truncate=0)

# Export analysis results to CSV
analysis_pd = analysisResult_df.toPandas()
analysis_pd.to_csv("analysis_results.csv", index=False)
print("Analysis results exported to analysis_results.csv")

"""
Run verification checks on good data
"""
print("\n=== DATA VERIFICATION RESULTS ===")

# Implement verification checks

verification_result = VerificationSuite(spark) \
    .onData(core_movie_df) \
    .addCheck(
        Check(spark, CheckLevel.Error, "Data quality checks for movie data")
            #check whether there is any duplicate record for movie, user combination
            .hasUniqueness(["movie_id", "user_id"], lambda x: x == 1)
            # Check for rating column with values between 0 and 5
            .hasMin("rating", lambda x: x >= 0.0)
            .hasMax("rating", lambda x: x <= 5.0)

            # Check genre columns have values between 0 and 1
            .hasMin("Action", lambda x: x >= 0.0)
            .hasMax("Action", lambda x: x <= 1.0)
            .hasMin("Comedy", lambda x: x >= 0.0)
            .hasMax("Comedy", lambda x: x <= 1.0)
            .hasMin("Crime", lambda x: x >= 0.0)
            .hasMax("Crime", lambda x: x <= 1.0)
            
            # Check for no null values in all columns
            .isComplete("user_id")
            .isComplete("movie_id")
            .isComplete("rating")
            .isComplete("title")
            .isComplete("Action")
            .isComplete("Comedy")
            .isComplete("Crime")
    ) \
    .run()

# Get verification results
verification_df = VerificationResult.checkResultsAsDataFrame(spark, verification_result)

# Create a cleaner version with better column names
clean_verification = verification_df.select(
    "check",
    "constraint", 
    verification_df.constraint_status.alias("passed"),
    "constraint_message"
)

# Configure display options to avoid truncation
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
spark.conf.set("spark.sql.repl.eagerEval.truncate", 0)

# Display clean results in console
clean_verification.show(n=100, truncate=0)





"""
Run verification on bad data for comparison
"""
print("\n=== BAD DATA VERIFICATION RESULTS ===")

# Run same verification on bad data
verification_result_bad = VerificationSuite(spark) \
    .onData(core_movie_df_bad) \
    .addCheck(
        Check(spark, CheckLevel.Error, "Data quality checks for movie data")

            .hasUniqueness(["movie_id", "user_id"], lambda x: x == 1)
            .hasMin("rating", lambda x: x >= 0.0)
            .hasMax("rating", lambda x: x <= 5.0)
            .hasMin("Action", lambda x: x >= 0.0)
            .hasMax("Action", lambda x: x <= 1.0)
            .hasMin("Comedy", lambda x: x >= 0.0)
            .hasMax("Comedy", lambda x: x <= 1.0)
            .hasMin("Crime", lambda x: x >= 0.0)
            .hasMax("Crime", lambda x: x <= 1.0)
            .isComplete("user_id")
            .isComplete("movie_id")
            .isComplete("rating")
            .isComplete("title")
            .isComplete("Action")
            .isComplete("Comedy")
            .isComplete("Crime")
    ) \
    .run()

# Get verification results for bad data
verification_df_bad = VerificationResult.checkResultsAsDataFrame(spark, verification_result_bad)

# Create a cleaner version for bad data
clean_verification_bad = verification_df_bad.select(
    "check",
    "constraint", 
    verification_df_bad.constraint_status.alias("passed"),
    "constraint_message"
)

# Display clean results for bad data
clean_verification_bad.show(n=100, truncate=0)



# Display overall verification status for bad data


# Count passed and failed checks for bad data




# Stop Spark session
spark.stop()