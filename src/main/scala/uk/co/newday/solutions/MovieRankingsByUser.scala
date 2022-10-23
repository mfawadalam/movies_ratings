package uk.co.newday.solutions

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object MovieRankingsByUser {

  def execute(movies: DataFrame, ratings: DataFrame): (DataFrame) = {
    // Complete the exercise and show the top 3 movies per user.
    val window = Window.partitionBy("userId").orderBy(col("rating").desc, col("movieId"))
    val topThreeByUser = ratings.select(col("userId"), col("movieId"),col("rating"),row_number().over(window).as("rowNumber")).where(col("rowNumber")<=3)
    val topThreeByUserMovieTitle = topThreeByUser.
      join(movies,topThreeByUser("movieId") === movies("movieId"), "left").
      select(col("userId"),topThreeByUser("movieId"),col("title"))
    topThreeByUserMovieTitle

  }

}
