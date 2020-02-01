name := "Spark_project"

version := "0.1"

scalaVersion := "2.12.6"

val SPARK_VERSION = "2.4.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % SPARK_VERSION,
  "org.apache.spark" %% "spark-sql" % SPARK_VERSION,
  "com.fasterxml.jackson.core" % "jackson-core" % "2.10.2"
)