package org.renci.umls

import java.io.{File, FileOutputStream, FileWriter, PrintStream}

import org.rogach.scallop._
import org.rogach.scallop.exceptions._
import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.renci.umls.rrf.RRFDir

import scala.io.Source

/**
  * Map terms from one code system to another.
  */
object CodeMapper extends App with LazyLogging {
  /**
    * Command line configuration for CodeMapper.
    */
  class Conf(arguments: Seq[String], logger: Logger) extends ScallopConf(arguments) {
    override def onError(e: Throwable): Unit = e match {
      case ScallopException(message) =>
        printHelp
        logger.error(message)
        System.exit(1)
      case ex => super.onError(ex)
    }

    val version = getClass.getPackage.getImplementationVersion
    version(s"CodeMapper: map from one source to another (v$version)")

    val rrfDir: ScallopOption[File] = opt[File](
      descr = "Directory of RRF files (called 'META' in the UMLS download)",
      default = Some(new File("./META"))
    )

    val sqliteDb: ScallopOption[File] = opt[File](
      descr = "SQLite file used as a backend database",
      default = Some(new File("./sqlite.db"))
    )

    val fromSource: ScallopOption[String] = opt[String](
      descr = "The source to translate from"
    )

    val toSource: ScallopOption[String] = opt[String](
      descr = "The source to translate to"
    )

    val idFile: ScallopOption[File] = opt[File](
      descr = "A file containing identifiers (in a single, newline-delimited column)"
    )

    val outputFile: ScallopOption[File] = opt[File](
      descr = "Where to write the output file"
    )

    verify()
  }

  // Parse command line arguments.
  val conf = new Conf(args.toIndexedSeq, logger)

  // Read RRF directory.
  val rrfDir = new RRFDir(conf.rrfDir(), conf.sqliteDb())
  logger.info(s"Loaded directory for release: ${rrfDir.releaseInfo}")
  logger.info(s"Using SQLite backend: ${rrfDir.sqliteDb}")

  val concepts = rrfDir.concepts
  val sources = concepts.getSources

  if (conf.fromSource.isEmpty && conf.toSource.isEmpty) {
    logger.info("Sources:")
    sources.map(entry => {
      logger.info(s" - ${entry._1} (${entry._2} entries)")
    })
  } else if (conf.fromSource.isEmpty) {
    // We know sourceTo is set.
    logger.error(s"--source-from is empty, although --source-to is set to '${conf.toSource()}'")
  } else if (conf.toSource.isEmpty) {
    // We know sourceFrom is set.
    logger.error(s"--source-to is empty, although --source-from is set to '${conf.fromSource()}'")
  } else {
    // Do we need to filter first?

    // Get ready to write output!
    val stream = if (conf.outputFile.isEmpty) System.out else new PrintStream(new FileOutputStream(conf.outputFile()))

    // Both sourceFrom and sourceTo are set!
    if (conf.idFile.isEmpty) {
      val map = concepts.getMap(conf.fromSource(), Seq.empty, conf.toSource(), Seq.empty)
      map.foreach(stream.println(_))
    } else {
      val ids = Source.fromFile(conf.idFile()).getLines.map(_.trim).toSeq
      logger.info(s"Filtering to ${ids.size} IDs from ${conf.idFile()}.")

      val map = concepts.getMap(conf.fromSource(), ids, conf.toSource(), Seq.empty)

      stream.println("id\tcount\tterm\tlabels\tparentTerms\tparentLabels")

      val mapByFromId = map.groupBy(_.fromCode)
      val matched = ids.map(id => {
        val maps = mapByFromId.getOrElse(id, Seq())
        val parentAtomIds = rrfDir.hierarchy.getParents(maps.flatMap(_.atomIds))
        val parentCUIs = concepts.getCUIsForAUI(parentAtomIds.toSeq)
        val halfMaps = concepts.getHalfMaps(conf.toSource(), parentCUIs.toSeq)
        stream.println(
          s"$id\t${maps.size}"
            + s"\t${maps.map(m => m.toSource + ":" + m.toCode)}"
            + s"\t${maps.map(_.labels).mkString("|")}"
            + s"\t${halfMaps}"
        )
        maps
      }).filter(_.nonEmpty)

      val percentage = (matched.size.toFloat/ids.size) * 100
      logger.info(f"Matched ${matched.size} IDs out of ${ids.size} ($percentage%.2f%%)")
    }

    stream.close()
  }
}
