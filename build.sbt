import sbt.Keys._
import sbt._

organization := "co.insilica"

scalaVersion := "2.11.8"

name := "sparktools"

version := "0.6.0"

// fork a new JVM for 'test:run', but not 'run'
fork in Test := true

// define the repository to publish to
publishTo := Some("S3" at "s3://s3-us-east-1.amazonaws.com/insilicaresolver")

// Exclude transitive dependencies, e.g., include log4j without including logging via jdmk, jmx, or jms.
libraryDependencies ++= Seq(

  //testing only
  "org.scalactic" %% "scalactic" % "3.0.1" % Test,
  "org.scalatest" %% "scalatest" % "3.0.1" % Test,

  "co.insilica" %% "functional" % "1.0",

  // Spark
  "org.apache.spark" %% "spark-core" % "2.2.0" % "provided",
  "org.apache.spark" %% "spark-hive" % "2.2.0" % "provided",
  "org.apache.spark" %% "spark-mllib" % "2.2.0" % "provided"
)
