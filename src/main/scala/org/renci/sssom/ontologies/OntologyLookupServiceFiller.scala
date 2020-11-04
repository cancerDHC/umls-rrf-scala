package org.renci.sssom.ontologies

import java.util

import org.renci.sssom.SSSOMFiller
import org.renci.sssom.SSSOMFiller.Row

import scala.jdk.CollectionConverters._
import uk.ac.ebi.pride.utilities.ols.web.service.client.OLSClient
import uk.ac.ebi.pride.utilities.ols.web.service.config.OLSWsConfigProd
import uk.ac.ebi.pride.utilities.ols.web.service.model.Term

/**
  * A generic class for filling in SSSOM files with terms from the Ontology Lookup Service (OLS).
  *
  * We use the API at https://www.ebi.ac.uk/ols/docs/api via the OLS Client
  * (https://github.com/PRIDE-Utilities/ols-client)
  */
class OntologyLookupServiceFiller extends SSSOMFiller {
  val client = new OLSClient(new OLSWsConfigProd())
  scribe.info(s"Loaded OLS Client with config: ${client.getConfig.toString}.")

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
      term <- {
        val result: util.List[Term] = client.getExactTermsByName(label, null)
        if (result == null) Seq() else result.asScala.distinctBy(_.getIri.getIdentifier)
      }
    } yield SSSOMFiller.Result(
      row,
      row + ("object_id" -> term.getIri.getIdentifier)
        + ("object_label" -> term.getLabel)
        + ("predicate_id" -> "skos:exactMatch")
        + ("predicate_label" -> "The subject and the object can, with a high degree of confidence, be used interchangeably across a wide range of information retrieval applications.")
        + ("comment" -> s"The EBI Ontology Lookup Service suggested this term as an exact match for label '$label'.")
        + ("match_type" -> "http://purl.org/sssom/type/Complex")
        + ("mapping_set_id" -> "https://www.ebi.ac.uk/ols/"),
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
