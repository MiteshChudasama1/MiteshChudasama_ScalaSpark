name := """UbsCodingAssignment"""

version := "1.0"

scalaVersion := "2.11.8"

lazy val spark = "2.3.1"


// Spark related dependencies

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % spark,
  "org.apache.spark" %% "spark-sql" % spark
)

fork in run := false