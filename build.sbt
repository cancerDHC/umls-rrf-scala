// Publication information
name         := "umls-rrf"
organization := "com.ggvaidya"
version      := "0.1-SNAPSHOT"

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

    // We need JDBC connection pooling so we can start new connections as needed.
    "org.apache.commons"          % "commons-dbcp2"           % "2.7.0",

  // Add support for SQLite via JDBC
    "org.xerial"                  % "sqlite-jdbc"             % "3.30.1",

    // Add support for calculating hashes for files.
    "commons-codec"               % "commons-codec"           % "1.14",

    // Add methods for storing information in-memory.
    "com.github.cb372"            %% "scalacache-core"        % "0.28.0",
    "com.github.cb372"            %% "scalacache-caffeine"    % "0.28.0",
    "com.github.ben-manes.caffeine" % "caffeine"              % "2.8.1",

  // Testing
    "com.lihaoyi"                 %% "utest"                  % "0.7.1" % "test"
  )
}
