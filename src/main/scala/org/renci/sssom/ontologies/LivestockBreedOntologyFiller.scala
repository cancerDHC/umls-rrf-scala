package org.renci.sssom.ontologies

import org.renci.sssom.SSSOMFiller
import org.renci.sssom.SSSOMFiller.Row

/**
  * Looks up terms in the Livestock Breed Ontology (http://bioportal.bioontology.org/ontologies/LBO).
  * We need a custom handler for this because a lot of our terms are in the format "Jamaica Red cattle breed"
  * while LBO has it in the format "Jamaica Red" (subclass of "cattle breed", http://purl.obolibrary.org/obo/LBO_0000144).
  */
class LivestockBreedOntologyFiller extends SSSOMFiller {
  override def toString: String = "LivestockBreedOntologyFiller()"

  /**
    * Fill in the input row. The list of all headers is also provided.
    *
    * @return None if this row could not be filled, and Some[Row] if it can.
    */
  override def fillRow(row: Row, headers: List[String]): Option[Seq[SSSOMFiller.Result]] = {
    val subjectId = row.getOrElse("subject_id", "(none)")
    val subjectLabels = row.getOrElse("subject_label", "").split("\\s*\\|\\s*").toSet

    val regex = "^(.*) cattle breed$".r
    subjectLabels.to(LazyList).flatMap({
      case regex(breedName) => Some(Seq(SSSOMFiller.Result(row, row, this)))
    }).headOption
  }
}
