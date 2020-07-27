package org.renci.umls

import java.io.{File, FileOutputStream, PrintStream}

import org.rogach.scallop._
import org.rogach.scallop.exceptions._
import org.renci.umls.rrf.RRFDir

import scala.concurrent.duration.Duration
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
  scribe.info(s"UMLS release name: ${rrfDir.releaseName}.")
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
    // Get ready to write output!
    val stream =
      if (conf.outputFile.isEmpty) System.out
      else new PrintStream(new FileOutputStream(conf.outputFile()))

    // Both sourceFrom and sourceTo are set!
    if (conf.idFile.isEmpty) {
      val maps = concepts.getMap(conf.fromSource(), Seq.empty, conf.toSource(), Seq.empty)
      stream.println(
        "subject_id\tsubject_label\tpredicate_id\tpredicate_label\tobject_id\tobject_label\tmatch_type\tmapping_set_id\tmapping_set_version\tcomment"
      )
      maps.foreach(map => {
        stream.println(
          s"${map.fromSource}:${map.fromCode}\t${concepts.getLabelsForCode(map.fromSource, map.fromCode).mkString("|")}\t" +
            s"skos:exactMatch\tThe subject and the object can, with a high degree of confidence, be used interchangeably across a wide range of information retrieval applications.\t" +
            s"${map.toSource}:${map.toCode}\t${concepts.getLabelsForCode(map.toSource, map.toCode).mkString("|")}\t" +
            s"http://purl.org/sssom/type/Complex\t" +
            s"https://ncim.nci.nih.gov/ncimbrowser/\t${rrfDir.releaseName}\t" +
            s"Both subject and object are mapped to NCI Metathesaurus CUIs: ${map.conceptIds.mkString("|")}"
        )
      })
    } else {
      val ids = Source.fromFile(conf.idFile()).getLines.map(_.trim).toSeq
      scribe.info(s"Filtering to ${ids.size} IDs from ${conf.idFile()}.")

      val map = concepts.getMap(conf.fromSource(), ids, conf.toSource(), Seq.empty)
      val allTermCuis = concepts.getCUIsForCodes(conf.fromSource(), ids)

      stream.println(
        "subject_id\tsubject_label\tpredicate_id\tpredicate_label\tobject_id\tobject_label\tmatch_type\tmapping_set_id\tmapping_set_version\tcomment"
      )

      var count = 0
      var countNoMatch = 0
      var countMatchDirect = 0
      var countMatchViaParent = 0

      val startTime = System.nanoTime()

      val mapByFromId = map.groupBy(_.fromCode)
      ids.foreach(id => {
        count += 1

        if (count % 100 == 0) {
          scribe.info(
            f"Processing ID ${conf.fromSource()}:$id ($count out of ${ids.size}: ${count / ids.size.toFloat * 100}%.2f%%)"
          )
        }

        val maps = mapByFromId.getOrElse(id, Seq())
        if (maps.nonEmpty) {
          countMatchDirect += 1
          maps.foreach(map => {
            stream.println(
              s"${conf.fromSource()}:$id\t${concepts.getLabelsForCode(conf.fromSource(), id).mkString("|")}\t" +
                s"skos:exactMatch\tThe subject and the object can, with a high degree of confidence, be used interchangeably across a wide range of information retrieval applications.\t" +
                s"${map.toSource}:${map.toCode}\t${concepts.getLabelsForCode(map.toSource, map.toCode).mkString("|")}\t" +
                s"http://purl.org/sssom/type/Complex\t" +
                s"https://ncim.nci.nih.gov/ncimbrowser/\t${rrfDir.releaseName}\t" +
                s"Both subject and object are mapped to NCI Metathesaurus CUIs: ${map.conceptIds.mkString("|")}"
            )
          })
        } else {
          val termCuis = allTermCuis.getOrElse(id, Seq.empty)
          // scribe.info(s"Checking $termCuis for parent AUI information.")

          val termAtomIds = concepts.getAUIsForCUIs(termCuis)
          val parentAtomIds = rrfDir.hierarchy.getParents(termAtomIds.toSet)
          val parentCUIs = concepts.getCUIsForAUI(parentAtomIds.toSeq)
          if (parentCUIs.isEmpty) {
            stream.println(
              s"${conf.fromSource()}:$id\t${concepts.getLabelsForCode(conf.fromSource(), id).mkString("|")}"
            )

            countNoMatch += 1
          } else {
            val halfMaps = concepts.getHalfMapsByCUIs(parentCUIs.toSet, conf.toSource())

            if (halfMaps.nonEmpty) {
              val sources: Set[(String, String, String)] =
                halfMaps.map(hm => (hm.source, hm.code, hm.cui)).toSet

              sources.foreach({
                case (toSource, toId, toCui) =>
                  stream.println(
                    s"${conf.fromSource()}:$id\t${concepts.getLabelsForCode(conf.fromSource(), id).mkString("|")}\t" +
                      s"skos:narrowMatch\tThe subject is taxonomically narrower than the object.\t" +
                      s"$toSource:$toId\t${concepts.getLabelsForCode(toSource, toId).mkString("|")}\t" +
                      s"http://purl.org/sssom/type/Complex\t" +
                      s"https://ncim.nci.nih.gov/ncimbrowser/\t${rrfDir.releaseName}\t" +
                      s"The subject (CUI ${termCuis.toSet
                        .mkString("|")}) has a parent term (CUI ${parentCUIs.toSet.mkString("|")}) " +
                      s"that can be mapped to CUIs in the output source (CUI $toCui)"
                  )
              })

              countMatchViaParent += 1
            } else {
              stream.println(
                s"${conf.fromSource()}:$id\t${concepts.getLabelsForCode(conf.fromSource(), id).mkString("|")}"
              )

              countNoMatch += 1
            }
          }
        }
      })

      val duration = Duration.fromNanos(System.nanoTime() - startTime)
      scribe.info(
        f"Processed $count IDs in ${duration.toMinutes} mins (${duration.toSeconds / count.toFloat}%.2f seconds per ID)"
      )
      scribe.info(
        f"Of these, $countMatchDirect (${countMatchDirect / count.toFloat * 100}%.2f%%) were matched directly, and $countMatchViaParent (${countMatchViaParent / count.toFloat * 100}%.2f%%) were matched via parent."
      )
    }

    stream.close()
  }
}
