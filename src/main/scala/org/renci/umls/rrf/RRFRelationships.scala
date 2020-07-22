package org.renci.umls.rrf

import java.io.File

/** Represents a single hierarchy entry. */
case class Relationship(
  ConceptId1: String, // CUI1
  AtomId1: String, // AUI1
  SourceType1: String, // STYPE1
  Relationship: String, // REL
  ConceptId2: String, // CUI2
  AtomId2: String, // AUI2
  SourceType2: String, // STYPE2
  SpecificRelationship: String, // RELA
  RelationshipId: String, // RUI
  SourceRelationshipId: String, // SRUI
  AbbrSourceName: String, // SAB
  RelationshipLabelSource: String, // SL
  RelationshipGroup: String, // RG
  SourceAssertedDirectionality: String, // DIR
  Suppressible: String, // SUPPRESS
  ContentViewFlag: String // CVF
)

/**
  * The RRFRelationships file contains relationship information on atoms in the system.
  */
class RRFRelationships(file: File, filename: String = "MRREL.RRF") extends RRFFile(file, filename) {

  /** A list of all relationships in an MRREL file. */
  lazy val relationships: Seq[Relationship] = {
    // We'll just hard-code this for now.
    // Eventually, it'd be nice to have this automatically settable from MRCOLS.RRF itself, but
    // right now I just don't have the time.
    rows.map(
      arr =>
        Relationship(
          arr(0),
          arr(1),
          arr(2),
          arr(3),
          arr(4),
          arr(5),
          arr(6),
          arr(7),
          arr(8),
          arr(9),
          arr(10),
          arr(11),
          arr(12),
          arr(13),
          arr(14),
          arr(15)
        )
    )
  }

}

object RRFRelationships {

  /** Wrap an RRF file as an RRFHierarchy. */
  def fromRRF(rrfFile: RRFFile) = new RRFRelationships(rrfFile.file, rrfFile.filename)
}
