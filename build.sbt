import Dependencies._

ThisBuild / scalaVersion     := s"$scalaCompat"
ThisBuild / version          := "0.1.0"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val scalaLab =
  Project(id = "scalalab", base = file("."))
    .disablePlugins(AssemblyPlugin)
    .settings(name := "scalalab")
    .aggregate(labRunner, labTR01)

val labRunnerMain = Some("example.com.scalalab.lab-runner.Main")

lazy val labRunner =
  (project in file("./lab-runner"))
    .enablePlugins(AssemblyPlugin)
    .settings(
      mainClass in (Compile, run) := labRunnerMain,
      mainClass in assembly := labRunnerMain,
      assemblyJarName in assembly := s"lab-runner-assembly.jar"
    )

val labTR01Main = Some("example.com.scalalab.lab-tr01.Main")

lazy val labTR01 =
  (project in file("./lab-tr01"))
    .enablePlugins(AssemblyPlugin)
    .settings(
      mainClass in (Compile, run) := labTR01Main,
      mainClass in assembly := labTR01Main,
      assemblyJarName in assembly := s"lab-tr01-assembly.jar",
      libraryDependencies ++= Seq(
        Spark.Core
      )
    )

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
