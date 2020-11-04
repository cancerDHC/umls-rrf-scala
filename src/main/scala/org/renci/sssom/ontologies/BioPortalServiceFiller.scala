package org.renci.sssom.ontologies

import java.io.{File, FileReader}
import java.util.Properties

import org.renci.sssom.SSSOMFiller
import org.renci.sssom.SSSOMFiller.Row

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/**
  * A generic class for filling in SSSOM files with terms from the BioPortal search API.
  *
  * We use the API at http://data.bioontology.org/documentation#nav_search
  */
class BioPortalServiceFiller extends SSSOMFiller {
  val configFile = new File(System.getProperty("user.home"), ".bioportal.properties")
  if (!configFile.canRead) throw new RuntimeException(s"Cannot read necessary BioPortal config file: ${configFile}")

  val configFileReader = new FileReader(configFile)
  val config = new Properties()
  config.load(configFileReader)
  configFileReader.close()

  val apiKey = config.getOrDefault("api_key", "").toString.trim
  if (apiKey.isEmpty) throw new RuntimeException(s"Configuration file ${configFile} does not contain 'api_key' property.")

  // scribe.info(s"API key found: ${apiKey}")

  scribe.info(s"Ready to use BioPortal API with config: ${configFile}.")

  /**
    * Return a list of terms that found with an exact search by BioPortal.
    */
  def getExactSearchFromBioPortal(label: String): Seq[Map[String, String]] = {
    val r = Try { requests.get("http://data.bioontology.org/search", params = Map(
      "q" -> label,
      "require_exact_match" -> "true",
      "apikey" -> apiKey,
      "format" -> "json",
      "pagesize" -> "100"
    )) }
    if (r.isFailure) {
      scribe.error(s"Could not connect to BioPortal search API: ${r.failed.get}")
      return Seq()
    }
    // scribe.info("Result: " + r.text())
    val parsed = ujson.read(r.get.text())
    return parsed("collection").arr
      // TODO fix hack: we need some way of not just mapping terms back to SNOMED!
      .filter(term => !term("@id").str.startsWith("http://purl.bioontology.org/ontology/SNOMEDCT/"))
      .filter(term => !term("obsolete").bool)
      .map(term => {
        val prefLabel: Option[String] = term.obj.get("prefLabel").map(_.str)
        val synonyms: Option[ArrayBuffer[String]] = term.obj.get("synonym").map(_.arr.map(_.str))
        // TODO: uh, fix this mess.
        val labels: Seq[String] = (prefLabel, synonyms) match {
          case (Some(prefLabel), Some(synonyms)) => (prefLabel +: synonyms).toSeq
          case (Some(prefLabel), None) => Seq(prefLabel)
          case (None, Some(synonyms)) => synonyms.toSeq
          case (None, None) => Seq()
        }

        Map(
          "predicate_id" -> term("@id").str,
          "predicate_label" -> labels.mkString("|"),
          "comment" -> s"BioPortal suggested this term as an exact match for label '$label' with matchType '${term("matchType")}'.",
          "match_type" -> "http://purl.org/sssom/type/Complex",
          "mapping_set_id" -> "http://data.bioontology.org/documentation#nav_search"
        )
      }).toSeq
  }

  /**
    * Extract subject label for a particular row
    */
  def extractSubjectLabelsFromRow(row: Row): Seq[String] = {
    row.getOrElse("subject_label", "").split("\\s*\\|\\s*").toSeq
  }

  /**
    * Find all subjects that match a particular row.
    */
  def identifyResultsForRow(row: Row): Seq[SSSOMFiller.Result] = {
    val labels = extractSubjectLabelsFromRow(row)

    // Do exact matches.
    val exactMatches = for {
      label <- labels
      term <- getExactSearchFromBioPortal(label)
    } yield SSSOMFiller.Result(
      row,
      row ++ term,
      this
    )

    exactMatches

    // If we don't have exact matches, do a non-exact match.
    /*
    if (exactMatches.nonEmpty) exactMatches else {
      val approxMatches = for {
        label <- labels
        term <- {
          val result: util.List[Term] = client.getTermsByName(label, null, false)
          if (result == null) Seq() else result.asScala.distinctBy(_.getIri.getIdentifier)
        }
      } yield SSSOMFiller.Result(
        row,
        row + ("object_id" -> term.getIri.getIdentifier)
          + ("object_label" -> term.getLabel)
          + ("predicate_id" -> "skos:relatedMatch")
          + ("predicate_label" -> "The subject and the object are associated in some unspecified way.")
          + ("comment" -> s"The EBI Ontology Lookup Service suggested this term as a non-exact match for label '$label'.")
          + ("match_type" -> "http://purl.org/sssom/type/Complex")
          + ("mapping_set_id" -> "https://www.ebi.ac.uk/ols/"),
        this
      )

      approxMatches
    }*/
  }

  /**
    * Fill in the input row. The list of all headers is also provided.
    *
    * @return None if this row could not be filled, and Some[Row] if it can.
    */
  override def fillRow(row: Row, headers: List[String]): Option[Seq[SSSOMFiller.Result]] = {
    val results = identifyResultsForRow(row)
    if (results.isEmpty) None else Some(results)
  }
}
