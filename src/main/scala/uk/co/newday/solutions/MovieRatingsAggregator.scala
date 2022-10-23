package uk.co.newday.solutions

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object MovieRatingsAggregator {

  def execute(movies: DataFrame, ratings: DataFrame): (DataFrame) = {
    //With two dataframe apply the join as specified in the exercise.
    //    movies.join(ratings,$"moviesmovieId" === $"ratingsmovieId","left")
    val movieRatings = movies.join(ratings, movies("movieId") === ratings("movieId"), "left")
    val minMaxAvgRatingByMovie = movieRatings.groupBy(movies("movieId"),movies("title"),movies("genre")).
      agg(min("rating").as("minRating"), max("rating").as("maxRating"), avg("rating").as("avgRating"))
    minMaxAvgRatingByMovie

  }
}
