package uk.co.newday.solutions

import org.apache.spark.sql.{DataFrame, SparkSession}


object DataWriter {

  def execute(movies: DataFrame, ratings: DataFrame, movieRatings:DataFrame, ratingWithRankTop3:DataFrame, moviesOutputPath: String,
    ratingsOutputPath: String, movieRatingsOutputPath: String, ratingWithRankTop3OutputPath: String): Unit = {
    //write all the output in parquet format
    write(movies, moviesOutputPath)
    write(ratings, ratingsOutputPath)
    write(movieRatings, movieRatingsOutputPath)
    write(ratingWithRankTop3, ratingWithRankTop3OutputPath)

  }

  private def write(dataFrame: DataFrame, path: String): Unit = {
    dataFrame.write.mode("overwrite").parquet(path)
  }
}
