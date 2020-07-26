package org.renci.umls

import java.io.{File, FileOutputStream, PrintStream}

import org.rogach.scallop._
import org.rogach.scallop.exceptions._
import org.renci.umls.rrf.RRFDir

import scala.io.Source

/**
  * Map terms from one code system to another.
  */
object CodeMapper extends App {

  /**
    * Command line configuration for CodeMapper.
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
    version(s"CodeMapper: map from one source to another (v$version)")

    val rrfDir: ScallopOption[File] = opt[File](
      descr = "Directory of RRF files (called 'META' in the UMLS download)",
      default = Some(new File("./META"))
    )

    val sqliteDb: ScallopOption[File] = opt[File](
      descr = "SQLite file used as a backend database",
      default = Some(new File("./sqlite.db"))
    )

    val fromSource: ScallopOption[String] = opt[String](descr = "The source to translate from")

    val toSource: ScallopOption[String] = opt[String](descr = "The source to translate to")

    val idFile: ScallopOption[File] =
      opt[File](descr = "A file containing identifiers (in a single, newline-delimited column)")

    val outputFile: ScallopOption[File] = opt[File](descr = "Where to write the output file")

    verify()
  }

  // Parse command line arguments.
  val conf = new Conf(args.toIndexedSeq)

  // Read RRF directory.
  val rrfDir = new RRFDir(conf.rrfDir(), conf.sqliteDb())
  scribe.info(s"Loaded directory for release: ${rrfDir.releaseInfo}")
  scribe.info(s"Using SQLite backend: ${rrfDir.sqliteDb}")

  val concepts = rrfDir.concepts
  val sources = concepts.getSources

  if (conf.fromSource.isEmpty && conf.toSource.isEmpty) {
    scribe.info("Sources:")
    sources.map(entry => {
      scribe.info(s" - ${entry._1} (${entry._2} entries)")
    })
  } else if (conf.fromSource.isEmpty) {
    // We know sourceTo is set.
    scribe.error(s"--source-from is empty, although --source-to is set to '${conf.toSource()}'")
  } else if (conf.toSource.isEmpty) {
    // We know sourceFrom is set.
    scribe.error(s"--source-to is empty, although --source-from is set to '${conf.fromSource()}'")
  } else {
    // Do we need to filter first?

    // Get ready to write output!
    val stream =
      if (conf.outputFile.isEmpty) System.out
      else new PrintStream(new FileOutputStream(conf.outputFile()))

    // Both sourceFrom and sourceTo are set!
    if (conf.idFile.isEmpty) {
      val maps = concepts.getMap(conf.fromSource(), Seq.empty, conf.toSource(), Seq.empty)
      stream.println("fromSource\tfromCode\ttoSource\ttoCode\tnciMTConceptIds\tlabels")
      maps.foreach(map => {
        stream.println(
          s"${map.fromSource}\t${map.fromCode}\t" +
            s"${map.toSource}\t${map.toCode}\t" +
            s"${map.conceptIds.mkString(", ")}\t" +
            s"${map.labels.mkString("|")}"
        )
      })
    } else {
      val ids = Source.fromFile(conf.idFile()).getLines.map(_.trim).toSeq
      scribe.info(s"Filtering to ${ids.size} IDs from ${conf.idFile()}.")

      val halfMapByCode = concepts.getHalfMapsForCodes(conf.fromSource(), ids).groupBy(_.code)
      val map = concepts.getMap(conf.fromSource(), ids, conf.toSource(), Seq.empty)
      val allTermCuis = concepts.getCUIsForCodes(conf.fromSource(), ids)

      stream.println(
        "fromSource\tid\tcuis\tlabels\tcountDirect\tcountViaParent\ttoIds\ttoLabels\tparentCuis\tparentSource\tparentIds\tparentLabels"
      )

      var count = 0
      val mapByFromId = map.groupBy(_.fromCode)
      val matched = ids.map(id => {
        val maps = mapByFromId.getOrElse(id, Seq())
        val (parentStr, parentHalfMaps) =
          if (maps.nonEmpty) ("", Seq.empty)
          else {
            val termCuis = allTermCuis.getOrElse(id, Seq.empty)
            // scribe.info(s"Checking $termCuis for parent AUI information.")

            val termAtomIds = concepts.getAUIsForCUIs(termCuis)
            val parentAtomIds = rrfDir.hierarchy.getParents(termAtomIds.toSet)
            val parentCUIs = concepts.getCUIsForAUI(parentAtomIds.toSeq)
            val halfMaps =
              if (parentCUIs.isEmpty) Seq.empty
              else concepts.getMapsByCUIs(parentCUIs.toSeq, conf.toSource())

            val cuis = halfMaps.map(_.cui).toSet
            val sources = halfMaps.map(_.source).toSet
            val codes = halfMaps.map(_.code).toSet
            val labels = halfMaps.map(_.label).toSet

            (
              s"\t${cuis.mkString("|")}\t${sources.mkString("|")}\t${codes.mkString("|")}\t${labels
                .mkString("|")}",
              halfMaps
            )
          }

        val halfMaps = halfMapByCode.getOrElse(id, Seq())

        stream.println(
          s"${conf.fromSource()}\t$id\t${halfMaps.map(_.cui).toSet.mkString("|")}\t${halfMaps
            .map(_.label)
            .toSet
            .mkString("|")}\t${maps.size}\t${parentHalfMaps.size}"
            + s"\t${maps.map(m => m.toSource + ":" + m.toCode).mkString("|")}"
            + s"\t${maps.map(_.labels.mkString(";")).mkString("|")}"
            + s"$parentStr"
        )

        count += 1
        if (count % 100 == 0) {
          val percentage = count.toFloat / ids.size * 100
          scribe.info(f"Processed $count out of ${ids.size} IDs ($percentage%.2f%%)")
        }

        (maps, parentHalfMaps)
      })

      val matchedTerm = matched.filter(_._1.nonEmpty).flatMap(_._1)
      val matchedParent = matched.filter(_._2.nonEmpty).flatMap(_._2)
      val matchedTotal = matched.filter(m => m._1.nonEmpty || m._2.nonEmpty)

      val percentageTerm = (matchedTerm.size.toFloat / ids.size) * 100
      val percentageParent = (matchedParent.size.toFloat / ids.size) * 100
      val percentageTotal = (matchedTotal.size.toFloat / ids.size) * 100
      scribe.info(f"Matched ${matchedTerm.size} IDs out of ${ids.size} ($percentageTerm%.2f%%)")
      scribe.info(
        f"Matched a further ${matchedParent.size} IDs via the parent term ($percentageParent%.2f%%)"
      )
      scribe.info(
        f"Total coverage: ${matchedTotal.size} IDs out of ${ids.size} ($percentageTotal%.2f%%)"
      )
    }

    stream.close()
  }
}
