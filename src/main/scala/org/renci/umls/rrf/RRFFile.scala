package org.renci.umls.rrf

import java.io.File

import scala.io.Source

/**
  * Wraps a single RRF file.
  */
class RRFFile(val file: File, val filename: String) {
  /** A list of all rows in this file. */
  def rows: Seq[IndexedSeq[String]] = Source.fromFile(file).getLines.map(_.split("\\|").toIndexedSeq).toSeq
}
