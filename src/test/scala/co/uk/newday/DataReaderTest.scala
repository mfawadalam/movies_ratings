package co.uk.newday
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import uk.co.newday.solutions.DataReader


trait TestSparkSession {

  val sparkSession: SparkSession = SparkSession.builder().
    appName("test spark session").
    master("local[1]").
    getOrCreate()

}

class DataReaderTest extends AnyFunSuite with TestSparkSession {

  test("test DataReader.execute") {

    val (moviesDF, ratingsDf) = DataReader(sparkSession).
      execute("src/test/resources/test_movies.dat", "src/test/resources/test_ratings.dat")

    assert(moviesDF.count()==3)
    assert(ratingsDf.count()==6)

  }

}


