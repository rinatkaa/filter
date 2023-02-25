ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "filter"
  )
libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.2.11"
//libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.2"
//libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.3"
//libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "6.8.2"
//libraryDependencies += "org.postgresql" % "postgresql" % "42.3.3"
val sparkVersion = "2.4.7"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
)