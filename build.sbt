name := "quantexa-coding-assignment"

version := "1.0"

scalaVersion := "2.12.10"

// Spark dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.8",
  "org.apache.spark" %% "spark-sql" % "2.4.8",
  
  // Test dependencies
  "org.scalatest" %% "scalatest" % "3.1.0" % "test"
)

// Assembly settings
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
} 