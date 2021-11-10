import Dependencies._

ThisBuild / scalaVersion     := s"$scalaCompat"
ThisBuild / version          := "0.1.0"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val scalaLab =
  Project(id = "scalalab", base = file("."))
    .disablePlugins(AssemblyPlugin)
    .settings(name := "scalalab")
    .aggregate(labRunner, labTR01, labTR03)

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

val labTR01Main = Some("Main")

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

val labTR03Main = Some("Main")

lazy val labTR03 =
  (project in file("./lab-tr03"))
    .enablePlugins(AssemblyPlugin)
    .settings(
      mainClass in (Compile, run) := labTR03Main,
      mainClass in assembly := labTR03Main,
      assemblyJarName in assembly := s"lab-tr03-assembly.jar",
      libraryDependencies ++= Seq(
        Spark.Core,
        Spark.Sql,
        Other.scalaTest
      )
    )

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

