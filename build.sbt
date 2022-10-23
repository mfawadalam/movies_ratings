name := "movies_ratings"

version := "1.0"

scalaVersion := "2.12.12"

val sparkVersion = "3.0.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.2.14" % Test ,
//  "org.scalatest" %% "scalatest-funsuite" % "3.2.14" ,
   "com.holdenkarau" %% "spark-testing-base" % "3.0.0_1.1.2" % Test
)

artifactPath in packageBin in Compile := baseDirectory.value / "artefact" / "movie_ratings.jar"
