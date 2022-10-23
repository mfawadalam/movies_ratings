package co.uk.newday

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import uk.co.newday.solutions.MovieRatingsAggregator

class MovieRatingsAggregatorTest extends AnyFunSuite with TestSparkSession with Matchers  {

  test("test MovieRatingsAggregator.execute") {
    val movies = Seq((1, "Toy Story (1995)", "Animation|Children's|Comedy"),
      (2, "Jumanji (1995)", "Adventure|Children's|Fantasy"),
      (3, "Grumpier Old Men (1995)", "Comedy|Romance"))

    val ratings = Seq(
      (10, 1, 5, 978300760),
      (10, 2, 3, 978302109),
      (20, 1, 3, 978301968),
      (20, 2, 4, 978300275),
      (20, 3, 5, 978824291),
      (30, 3, 3, 978302268)
    )

    val moviesSchema = new StructType()
      .add(StructField("movieId", IntegerType))
      .add(StructField("title", StringType))
      .add(StructField("genre", StringType))

    val ratingsSchema = new StructType()
      .add(StructField("userId", IntegerType))
      .add(StructField("movieId", IntegerType))
      .add(StructField("rating", IntegerType))
      .add(StructField("timestamp", IntegerType))

    val moviesRdd = sparkSession.sparkContext.parallelize(movies).map(x => Row(x._1, x._2, x._3))
    val ratingsRdd = sparkSession.sparkContext.parallelize(ratings).map(x => Row(x._1, x._2, x._3, x._4))


    val moviesDf = sparkSession.createDataFrame(moviesRdd, moviesSchema)
    val ratingsDf = sparkSession.createDataFrame(ratingsRdd, ratingsSchema)

    val actualDf = MovieRatingsAggregator.execute(moviesDf,ratingsDf)

    val actual = actualDf.collect().
      map( row => (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4), row.get(5)))

    val expected = Array(
      (1, "Toy Story (1995)","Animation|Children's|Comedy",3,5,4.0),
      (2, "Jumanji (1995)","Adventure|Children's|Fantasy",3,4,3.5),
      (3, "Grumpier Old Men (1995)","Comedy|Romance",3,5,4.0)
    )

    actual should contain theSameElementsAs expected

  }

}
