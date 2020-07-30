package org.renci.sssom

import java.io.{File, FileWriter, OutputStreamWriter, PrintWriter}

import com.github.tototoshi.csv.{CSVReader, CSVWriter, TSVFormat}
import org.rogach.scallop.{ScallopConf, ScallopOption}
import org.rogach.scallop.exceptions.ScallopException

import scala.io.Source

/**
  * org.renci.sssom.Filler is an application for filling in gaps in an [SSSOM file](https://github.com/OBOFoundry/SSSOM).
  */
object Filler extends App {

  /**
    * Command line configuration for Filler.
    */
  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    override def onError(e: Throwable): Unit =
      e match {
        case ScallopException(message) =>
          printHelp
          scribe.error(message)
          System.exit(1)
        case ex => super.onError(ex)
      }

    val version = getClass.getPackage.getImplementationVersion
    version(s"Filler: add additional mappings to an existing SSSOM file (v$version)")

    val inputFile: ScallopOption[File] = opt[File](descr = "The SSSOM file to fill in")
    val outputFile: ScallopOption[File] = opt[File](descr = "The output file (in SSSOM format)")

    val fromOntology: ScallopOption[List[File]] = opt[List[File]](
      descr = "A list of ontologies to search for terms"
    )

    val fillPredicateId: ScallopOption[String] = opt[String](
      descr = "Choose the predicate ID to fill in (e.g. 'skos:narrowMatch')"
    )

    verify()
  }

  // Parse command line arguments.
  val conf = new Conf(args.toIndexedSeq)
  val inputSource = if(conf.inputFile.isSupplied) Source.fromFile(conf.inputFile()) else Source.fromInputStream(System.in, "UTF-8")
  val outputWriter = if(conf.outputFile.isSupplied) new PrintWriter(new FileWriter(conf.outputFile())) else new OutputStreamWriter(System.out)

  // Read input as TSV.
  val reader = CSVReader.open(inputSource)(new TSVFormat {})
  val (headers, rows) = reader.allWithOrderedHeaders()

  val writer = CSVWriter.open(outputWriter)(new TSVFormat {})
  writer.writeRow(headers)
  rows.map(row => {
    // Should we fill in this row?
    if(conf.fillPredicateId.isSupplied && row.getOrElse("predicate_id", "") == conf.fillPredicateId()) {
      // Apply all the SSSOMRowFillers that are relevant here.
      row
    } else row
  }).foreach(row => {
    // Write out each row in the correct order.
    writer.writeRow(headers.map(header => row.getOrElse(header, "")))
  })
  writer.close()
}