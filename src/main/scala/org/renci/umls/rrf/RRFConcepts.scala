package org.renci.umls.rrf

import java.io.File

/** Represents a single column entry. */
case class Concept(
  ConceptID: String,            // CUI
  Lang: String,                 // LAT
  TermStatus: String,           // TS
  TermID: String,               // LUI
  StringType: String,           // STT
  StringID: String,             // SUI
  IsPreferred: Boolean,         // ISPREF
  AtomID: String,               // AUI
  SourceAtomID: String,         // SAUI
  SourceConceptID: String,      // SCUI
  SourceDescriptorID: String,   // SDUI
  Source: String,               // SAB
  TermType: String,             // TTY
  SourceEntryID: String,        // CODE
  EntryString: String,          // STR
  SourceRestriction: String,    // SRL
  SuppressibleFlag: String,     // SUPPRESS
  ContentViewFlag: String       // CVF
)

/**
  * The RRFConcepts file allows you to read concept data from MRCONSO.RRF.
  */
class RRFConcepts(file: File, filename: String = "MRCONSO.RRF") extends RRFFile(file, filename) {
  /** A list of all columns in an RRFConcepts file. */
  val concepts: Seq[Concept] = {
    // We'll just hard-code this for now.
    // Eventually, it'd be nice to have this automatically settable from MRFILES.RRF itself, but
    // right now I just don't have the time.
    rows.map(arr => Concept(
      arr(0),
      arr(1),
      arr(2),
      arr(3),
      arr(4),
      arr(5),
      arr(6).trim match {
        case "Y" => true
        case _ => false
      },
      arr(7),
      arr(8),
      arr(9),
      arr(10),
      arr(12),
      arr(13),
      arr(14),
      arr(15),
      arr(16),
      arr(17),
      arr(18)
    ))
  }
}

object RRFConcepts {
  /** Wrap an RRF file as an RRFCols. */
  def fromRRF(rrfFile: RRFFile) = new RRFConcepts(rrfFile.file, rrfFile.filename)
}