// Publication information
name := "umls-rrf"
ThisBuild / organization := "com.ggvaidya"
ThisBuild / version      := "0.1-SNAPSHOT"

// Code license
licenses := Seq("MIT" -> url("https://opensource.org/licenses/MIT"))

// Scalac options.
scalaVersion := "2.13.1"
scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8", "-Ywarn-unused")

addCompilerPlugin(scalafixSemanticdb)
scalacOptions in Test ++= Seq("-Yrangepos")

// Set up the main class.
mainClass in (Compile, run) := Some("org.renci.umls.CodeMapper")

// Fork when running.
fork in run := true

// Set up testing.
testFrameworks += new TestFramework("utest.runner.Framework")

// Code formatting and linting tools.
wartremoverWarnings ++= Warts.unsafe

addCommandAlias(
  "scalafixCheckAll",
  "; compile:scalafix --check ; test:scalafix --check"
)

// Library dependencies.
libraryDependencies ++= {
  Seq(
    // Logging
    "com.typesafe.scala-logging"  %% "scala-logging"          % "3.9.2",
    "ch.qos.logback"              %  "logback-classic"        % "1.2.3",

    // Command line argument parsing.
    "org.rogach"                  %% "scallop"                % "3.3.2",

    // Import Apache Jena to read JSON-LD.
    // "org.apache.jena"             % "jena-core"               % "3.14.0",

    // https://mvnrepository.com/artifact/org.apache.jena/jena-arq
    // "org.apache.jena"             % "jena-arq"                % "3.14.0",

    // Add support for CSV
    "com.github.tototoshi"        %% "scala-csv"              % "1.3.6",

    // Add support for JDBC via Slick
    "com.typesafe.slick"          %% "slick"                  % "3.3.2",

    // Add support for SQLite via JDBC
    "org.xerial"                  % "sqlite-jdbc"             % "3.30.1",

    // Testing
    "com.lihaoyi"                 %% "utest"                  % "0.7.1" % "test"
  )
}
