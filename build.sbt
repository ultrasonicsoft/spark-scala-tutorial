name := "App4"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "org.bdgenomics.adam" %% "adam-core-spark2" % "0.30.0" exclude("com.google.guava", "guava")
)

