from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import sys


def computeCosineSimilarity(data):
    # Compute xx, xy, and yy columns
    pairScores = data \
        .withColumn("xx", func.col("rating1") * func.col("rating1")) \
        .withColumn("yy", func.col("rating2") * func.col("rating2")) \
        .withColumn("xy", func.col("rating1") * func.col("rating2"))

    # Compute numerator, denominator, and numPairs columns
    calculateSimilarity = pairScores \
        .groupBy("movie1", "movie2") \
        .agg(
            func.sum(func.col("xy")).alias("numerator"),
            (
                func.sqrt(func.sum(func.col("xx"))) *
                func.sqrt(func.sum(func.col("yy")))
            ).alias("denominator"),
            func.count(func.col("xy")).alias("numPairs")
        )

    # Calculate score
    result = calculateSimilarity \
        .withColumn(
            "score",
            func.when(
                func.col("denominator") != 0,
                func.col("numerator") / func.col("denominator")
            ).otherwise(0)
        ) \
        .select("movie1", "movie2", "score", "numPairs")

    return result


def getMovieName(movieNames, movieId):
    result = movieNames \
        .filter(func.col("movieID") == movieId) \
        .select("movieTitle") \
        .collect()

    if len(result) == 0:
        return f"Movie ID {movieId}"

    return result[0][0]


spark = SparkSession.builder \
    .appName("MovieSimilarities") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


movieNamesSchema = StructType([
    StructField("movieID", IntegerType(), True),
    StructField("movieTitle", StringType(), True)
])

moviesSchema = StructType([
    StructField("userID", IntegerType(), True),
    StructField("movieID", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])


# Load movie names from MovieLens 100k dataset
movieNames = spark.read \
    .option("sep", "|") \
    .option("charset", "ISO-8859-1") \
    .schema(movieNamesSchema) \
    .csv("./ml-100k/u.item")


# Load movie ratings from MovieLens 100k dataset
movies = spark.read \
    .option("sep", "\t") \
    .schema(moviesSchema) \
    .csv("./ml-100k/u.data")


ratings = movies.select("userID", "movieID", "rating")


# Self-join ratings to find every movie pair rated by the same user
moviePairs = ratings.alias("ratings1") \
    .join(
        ratings.alias("ratings2"),
        (func.col("ratings1.userID") == func.col("ratings2.userID")) &
        (func.col("ratings1.movieID") < func.col("ratings2.movieID"))
    ) \
    .select(
        func.col("ratings1.movieID").alias("movie1"),
        func.col("ratings2.movieID").alias("movie2"),
        func.col("ratings1.rating").alias("rating1"),
        func.col("ratings2.rating").alias("rating2")
    )


# Compute movie pair similarities
moviePairSimilarities = computeCosineSimilarity(moviePairs).cache()


# Save all movie pair similarities as Parquet
moviePairSimilarities.write \
    .mode("overwrite") \
    .parquet("output/movie-pair-similarities")


if len(sys.argv) > 1:
    scoreThreshold = 0.97
    coOccurrenceThreshold = 50

    movieID = int(sys.argv[1])

    filteredResults = moviePairSimilarities.filter(
        ((func.col("movie1") == movieID) | (func.col("movie2") == movieID)) &
        (func.col("score") > scoreThreshold) &
        (func.col("numPairs") > coOccurrenceThreshold)
    )

    # Save filtered results as Parquet
    filteredResults.write \
        .mode("overwrite") \
        .parquet(f"output/similar-movies-{movieID}")

    results = filteredResults \
        .sort(func.col("score").desc()) \
        .take(10)

    print("Top 10 similar movies for " + getMovieName(movieNames, movieID))

    for result in results:
        similarMovieID = result.movie1

        if similarMovieID == movieID:
            similarMovieID = result.movie2

        print(
            getMovieName(movieNames, similarMovieID) +
            "\tscore: " + str(result.score) +
            "\tstrength: " + str(result.numPairs)
        )


spark.stop()
