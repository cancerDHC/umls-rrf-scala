package org.renci.sssom

import java.io.{File, FileWriter, OutputStreamWriter, PrintWriter}

import com.github.tototoshi.csv.{CSVReader, CSVWriter, TSVFormat}
import org.renci.sssom.ontologies.{
  LivestockBreedOntologyFiller,
  OntologyFiller,
  OntologyLookupServiceFiller
}
import org.rogach.scallop.exceptions.ScallopException
import org.rogach.scallop.{ScallopConf, ScallopOption}

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
      descr = "A list of ontologies (as local RDF files) to search for terms",
      short = 'o',
      default = Some(List[File]())
    )

    val fillLivestockBreedOntology: ScallopOption[Boolean] =
      opt[Boolean](descr = "Fill terms from the Livestock Breed Ontology")

    val fillFromOls: ScallopOption[Boolean] =
      opt[Boolean](descr = "Fill terms from the Ontology Lookup Service")

    val fillPredicateId: ScallopOption[String] = opt[String](descr =
      "Choose the predicate ID to fill in (e.g. 'skos:narrowMatch') in addition to blank rows"
    )

    verify()
  }

  // Parse command line arguments.
  val conf = new Conf(args.toIndexedSeq)
  val inputSource =
    if (conf.inputFile.isSupplied) Source.fromFile(conf.inputFile())
    else Source.fromInputStream(System.in, "UTF-8")
  val outputWriter =
    if (conf.outputFile.isSupplied) new PrintWriter(new FileWriter(conf.outputFile()))
    else new OutputStreamWriter(System.out)

  // Build a list of SSSOMRowFillers that we need to apply here.
  val rowFillers: Seq[SSSOMFiller] = (
    (if (conf.fillFromOls()) Some(new OntologyLookupServiceFiller()) else None) +:
      (if (conf.fillLivestockBreedOntology()) Some(new LivestockBreedOntologyFiller()) else None) +:
      conf.fromOntology().map(file => Some(OntologyFiller(file)))
  ).flatten
  scribe.info(s"Active row fillers: ${rowFillers.mkString(", ")}")

  if (rowFillers.isEmpty) {
    scribe.error("No row fillers set! Use --help to see a list of possible filters.")
    System.exit(1)
  }

  // Read input as TSV.
  val reader = CSVReader.open(inputSource)(new TSVFormat {})
  val (headers, rows) = reader.allWithOrderedHeaders()

  // Counters
  var countExistingTerm = 0
  var countNoMatch = 0
  var countMatch = 0

  val writer = CSVWriter.open(outputWriter)(new TSVFormat {})
  writer.writeRow(headers)
  rows
    .to(LazyList)
    .flatMap(row => {
      // Should we fill in this row?
      val subjectId = row.getOrElse("subject_id", "(none)")
      val predicateId = row.getOrElse("predicate_id", "")
      if (
        predicateId.isEmpty || (conf.fillPredicateId.isSupplied && predicateId == conf
          .fillPredicateId())
      ) {
        scribe.debug(
          s"Looking for a match for ${row.getOrElse("subject_id", "")} (${row.getOrElse("subject_label", "")})"
        )
        val optMatchResult = rowFillers.to(LazyList).flatMap(_.fillRow(row, headers)).headOption
        if (optMatchResult.isEmpty) {
          scribe.debug(s"Could not fill subject ID $subjectId: no fillers matched")
          countNoMatch += 1
          Seq(row)
        } else {
          val result = optMatchResult.head
          val rows = result.map(_.result)
          scribe.info(s"Filled subject ID $subjectId with ${rows}")
          countMatch += 1
          rows
        }
      } else {
        // This term already has a pre-existing term.
        countExistingTerm += 1
        Seq(row)
      }
    })
    .foreach(row => {
      // Write out each row in the correct order.
      writer.writeRow(headers.map(header => row.getOrElse(header, "")))
    })
  writer.close()

  scribe.info(
    f"""Out of ${rows.size} rows:
       |  - $countExistingTerm rows (${countExistingTerm.toFloat / rows.size * 100}%.2f%%) have existing terms.
       |  - $countNoMatch rows (${countNoMatch.toFloat / rows.size * 100}%.2f%%) could not be matched.
       |  - $countMatch rows (${countMatch.toFloat / rows.size * 100}%.2f%%) could be matched.""".stripMargin
  )
}
