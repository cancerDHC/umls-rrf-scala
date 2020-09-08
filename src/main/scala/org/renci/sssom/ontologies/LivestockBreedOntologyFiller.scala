package org.renci.sssom.ontologies

import java.io.File

import org.renci.sssom.SSSOMFiller.Row

/**
  * Looks up terms in the Livestock Breed Ontology (http://bioportal.bioontology.org/ontologies/LBO).
  * We need a custom handler for this because a lot of our terms are in the format "Jamaica Red cattle breed"
  * while LBO has it in the format "Jamaica Red" (subclass of "cattle breed", http://purl.obolibrary.org/obo/LBO_0000144).
  */
class LivestockBreedOntologyFiller extends OntologyFiller(new File("ontologies/lbo.owl")) {
  override def toString: String = "LivestockBreedOntologyFiller()"

  override def extractSubjectLabelsFromRow(row: Row): Seq[String] = {
    val results = row.getOrElse("subject_label", "").split("\\s*\\|\\s*").flatMap(label => {
      // Try adding various breed labels at the end of it.
      Seq(
        label,
        label.replaceAll(" cattle breed$", ""),
        label.replaceAll(" cattle$", ""),
        label.replaceAll(" sheep breed$", ""),
        label.replaceAll(" sheep$", ""),
        label.replaceAll(" horse breed$", ""),
        label.replaceAll(" horse$", ""),
        label.replaceAll(" pig$", "")
      )
    }).toSeq
    scribe.info(s"Looking for subject label ${row.getOrElse("subject_label", "")}: $results")
    results
  }
}
