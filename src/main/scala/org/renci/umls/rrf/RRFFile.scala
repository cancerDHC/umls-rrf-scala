package org.renci.umls.rrf

import java.io.File

import scala.io.Source

/**
  * Wraps a single RRF file.
  */
class RRFFile(val file: File, val filename: String) {
  val cols: Seq[Array[String]] = Source.fromFile(file).getLines.map(_.split("\\|")).toSeq
}
