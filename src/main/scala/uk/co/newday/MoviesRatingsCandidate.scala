package uk.co.newday

import org.apache.commons.cli.CommandLineParser
import org.apache.spark.sql.{DataFrame, SparkSession}
import uk.co.newday.solutions.{DataReader, DataWriter, MovieRankingsByUser, MovieRatingsAggregator}


object MoviesRatingsCandidate {

  def main(args: Array[String]) {

//  Can use an options parser
    if (args.length != 2)
      {
        println("Usage: abc.jar [INPUTDIR] [OUTPUTDIR]")

      }

    val inputPath = args(0)
    val outputPath = args(1)


    val sparkSession: SparkSession = SparkSession.builder().
      appName("movie_ratings").
      getOrCreate()

    val moviesPath: String = s"$inputPath/movies.dat"
    val ratingsPath: String = s"$inputPath/ratings.dat"

    val moviesOutputPath: String = s"$outputPath/movies"
    val ratingsOutputPath: String = s"$outputPath/ratings"
    val movieRatingsOutputPath: String = s"$outputPath/movieRatings"
    val ratingWithRankTop3OutputPath: String = s"$outputPath/ratingWithRankTop3"

    val (movies, ratings): (DataFrame, DataFrame) = DataReader(sparkSession).execute(moviesPath, ratingsPath)

    val movieRatings: DataFrame = MovieRatingsAggregator.execute(movies, ratings)
    val ratingWithRankTop3: DataFrame = MovieRankingsByUser.execute(movies, ratings)
    DataWriter.execute(movies, ratings, movieRatings, ratingWithRankTop3,
      moviesOutputPath, ratingsOutputPath, movieRatingsOutputPath, ratingWithRankTop3OutputPath)

    sparkSession.stop()
  }
}
