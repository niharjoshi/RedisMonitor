name := "DynamoDBMonitor"

version := "0.1"

scalaVersion := "2.13.7"

val AkkaVersion = "2.6.17"
libraryDependencies += "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion

val AkkaHttpVersion = "10.2.7"
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
  "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
  "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion
)

libraryDependencies ++= Seq(
  "net.debasishg" %% "redisclient" % "3.42"
)
