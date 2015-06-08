name := """disruptor"""

version := "1.0"

lazy val commonSettings = Seq(
  scalaVersion := "2.11.6",
  organization := "com.example"
)

fork in run := true

lazy val root = (project in file(".")).
  configs(IntegrationTest).
  settings(commonSettings: _*).
  settings(Defaults.itSettings: _*).
  settings(
    // other settings here
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor" % "2.3.11",
      "com.typesafe.akka" %% "akka-testkit" % "2.3.11" % "it,test",
      "org.scalatest" %% "scalatest" % "2.2.4" % "it,test")
  )
