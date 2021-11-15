import Dependencies._

ThisBuild / scalaVersion     := s"$scalaCompat"
ThisBuild / version          := "0.1.0"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val scalaLab =
  Project(id = "scalalab", base = file("."))
    .disablePlugins(AssemblyPlugin)
    .settings(name := "scalalab")
    .aggregate(labRunner, labTR01, labTR02, labTR03)

val labRunnerMain = Some("Main")

lazy val labRunner =
  (project in file("./lab-runner"))
    .enablePlugins(AssemblyPlugin)
    .settings(
      mainClass in (Compile, run) := labRunnerMain,
      mainClass in assembly := labRunnerMain,
      assemblyJarName in assembly := s"lab-runner-assembly.jar",
      libraryDependencies ++= Seq(
        Other.gson,
        Other.scalaTest,
        Other.commonsCli
      )
    )


val labTR01Main = Some("com.example.scalalab.labTR01.Main")

lazy val labTR01 =
  (project in file("./lab-tr01"))
    .enablePlugins(AssemblyPlugin)
    .settings(
      Compile / run / mainClass := labTR01Main,
      assembly / mainClass  := labTR01Main,
      assembly / assemblyJarName  := s"lab-tr01.jar",
      libraryDependencies ++= Seq(
        Spark.Core,
        Spark.SQL,
        Postgres.driver
      )
    )

val labTR03Main = Some("com.example.scalalab.labTR03.Main")

lazy val labTR03 =
  (project in file("./lab-tr03"))
    .enablePlugins(AssemblyPlugin)
    .settings(
      mainClass in (Compile, run) := labTR03Main,
      mainClass in assembly := labTR03Main,
      assemblyJarName in assembly := s"lab-tr03-assembly.jar",
      libraryDependencies ++= Seq(
        Spark.Core,
        Spark.SQL,
        Other.scalaTest
      )
    )

val labTR02Main = Some("com.example.scalalab.labTR02.Main")
lazy val labTR02 =
  (project in file("./lab-tr02"))
    .enablePlugins(AssemblyPlugin)
    .settings(
      mainClass in (Compile, run) := labTR02Main,
      mainClass in assembly := labTR02Main,
      assemblyJarName in assembly := s"lab-tr02-assembly.jar",
      libraryDependencies ++= Seq(
        Spark.Core,
        Spark.SQL
      )
    )

ThisBuild / assemblyMergeStrategy := {
  case PathList("org",xs @ _*)         => MergeStrategy.first
  case PathList("javax",xs @ _*)         => MergeStrategy.first
  case x if x.contains(".properties") => MergeStrategy.first
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.

