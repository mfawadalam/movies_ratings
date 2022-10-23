package uk.co.newday.solutions

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}


class DataReader(val sparkSession: SparkSession) {

  private case class Movie(movieId: Int, title: String, genre: String)

  private case class Rating(userId: Int, movieId: Int, rating: Int, timestamp: Int)

  def execute(moviesPath: String, ratingsPath: String): (DataFrame, DataFrame) = {
    //Please load movies and ratings csv's in output dataframes.

    val movieSchema = Encoders.product[Movie].schema
    val ratingSchema = Encoders.product[Rating].schema

    val moviesDf = readCSV(moviesPath, movieSchema, Map("delimiter" -> "::", "header" -> "false"))

    val ratingsDf = readCSV(ratingsPath, ratingSchema, Map("delimiter" -> "::", "header" -> "false"))

    (moviesDf, ratingsDf)
  }

  private def readCSV(path: String, schema: StructType, options: Map[String, String]): DataFrame = {
    sparkSession.read.options(options).
      schema(schema).
      csv(path)
  }
}

object DataReader {

  def apply(sparkSession: SparkSession): DataReader = {
    new DataReader(sparkSession)
  }

}
