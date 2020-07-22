package org.renci.umls.rrf

import java.io.File

import scala.io.Source

import org.apache.commons.codec.digest.DigestUtils

/**
  * Wraps a single RRF file.
  */
class RRFFile(val file: File, val filename: String) {

  /** A list of all rows in this file. */
  lazy val rows: Seq[IndexedSeq[String]] =
    Source.fromFile(file).getLines.map(_.split("\\|").toIndexedSeq).toSeq

  /** Count the number of rows in this file. */
  lazy val rowCount: Long = Source.fromFile(file).getLines.size

  /** Calculate an SHA1 hash for this file. */
  lazy val sha256: String = new DigestUtils(DigestUtils.getSha256Digest).digestAsHex(file)
}
