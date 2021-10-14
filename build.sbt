import Dependencies._

ThisBuild / scalaVersion     := s"$scalaCompat"
ThisBuild / version          := "0.1.0"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"


lazy val scalaLab =
  Project(id = "scalalab", base = file("."))
    .disablePlugins(AssemblyPlugin)
    .settings(name := "scalalab")

val labRunnerMain = Some("example.com.scalalab.lab-runner.Main")

lazy val labRunner =
  (project in file("./lab-runner"))
    .enablePlugins(AssemblyPlugin)
    .settings(
      mainClass in (Compile, run) := labRunnerMain,
      mainClass in assembly := labRunnerMain,
      assemblyJarName in assembly := s"lab-runner-assembly.jar"
    )



// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
