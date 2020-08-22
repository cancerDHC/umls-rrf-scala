package org.renci.sssom.ontologies

import java.io.File

import org.renci.sssom.SSSOMFiller
import org.renci.sssom.SSSOMFiller.Row

/**
  * Looks up terms in the Livestock Breed Ontology (http://bioportal.bioontology.org/ontologies/LBO).
  * We need a custom handler for this because a lot of our terms are in the format "Jamaica Red cattle breed"
  * while LBO has it in the format "Jamaica Red" (subclass of "cattle breed", http://purl.obolibrary.org/obo/LBO_0000144).
  */
class LivestockBreedOntologyFiller extends OntologyFiller(new File("ontologies/lbo.owl")) {
  override def toString: String = "LivestockBreedOntologyFiller()"


}
