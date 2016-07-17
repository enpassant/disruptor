name := """disruptor"""

version := "1.0"

lazy val commonSettings = Seq(
  scalaVersion := "2.11.6",
  organization := "com.example"
)

scalacOptions ++= Seq(
  "-feature",
  "-unchecked",
  "-deprecation",
  "-Xlint"
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
      "org.fusesource.leveldbjni" % "leveldbjni-all" % "1.8",
      "org.scalatest" %% "scalatest" % "2.2.4" % "it,test",
      "org.json4s" %% "json4s-jackson" % "3.4.0",
      "org.json4s" %% "json4s-ext" % "3.4.0")
  )
