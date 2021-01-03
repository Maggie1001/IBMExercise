name := "SparkScalaExercise"

version := "1.0"

scalaVersion := "2.12.12"

val sparkVersion = "3.0.1"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "3.0.1",
  "com.ibm.stocator" % "stocator" % "1.1.3",
  "com.typesafe" % "config" % "1.4.1",
  "org.apache.spark" %% "spark-sql" % "3.0.1",
  "com.ibm.db2" % "jcc" % "11.5.5.0")
