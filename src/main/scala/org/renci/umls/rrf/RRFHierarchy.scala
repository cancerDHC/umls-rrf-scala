package org.renci.umls.rrf

import java.io.File

/** Represents a single hierarchy entry. */
case class HierarchyEntry(
  ConceptId: String,                  // CUI
  AtomId: String,                     // AUI
  ContextNumber: String,              // CXN
  ParentAtomId: String,               // PAUI
  Source: String,                     // SAB
  Relation: String,                   // RELA
  PathToRoot: String,                 // PTR
  HierarchyCode: String,              // HCD
  ContentViewFlag: String             // CVF
)

/**
  * The RRFHierarchy file contains hierarchy information on atoms in the system.
  */
class RRFHierarchy(file: File, filename: String = "MRHIER.RRF") extends RRFFile(file, filename) {
  /** A list of all columns in an RRFCols file. */
  lazy val columns: Seq[HierarchyEntry] = {
    // We'll just hard-code this for now.
    // Eventually, it'd be nice to have this automatically settable from MRCOLS.RRF itself, but
    // right now I just don't have the time.
    rows.map(arr => HierarchyEntry(
      arr(0),
      arr(1),
      arr(2),
      arr(3),
      arr(4),
      arr(5),
      arr(6),
      arr(7),
      arr(8)
    ))
  }

  def getParents(atomId: String): Set[String] = columns.filter(_.AtomId == atomId).map(_.ParentAtomId).toSet
  def getOnlyParent(atomId: String): String = {
    val set = getParents(atomId)
    if (set.size < 1) throw new RuntimeException(s"No parents found for atom ID: $atomId")
    if (set.size > 1) throw new RuntimeException(s"Too many parents found for atom ID: $atomId: $set")
    set.head
  }
}

object RRFHierarchy {
  /** Wrap an RRF file as an RRFHierarchy. */
  def fromRRF(rrfFile: RRFFile) = new RRFHierarchy(rrfFile.file, rrfFile.filename)
}