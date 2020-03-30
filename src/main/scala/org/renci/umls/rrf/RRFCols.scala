package org.renci.umls.rrf

import java.io.File

/**
  * The RRFCols file contains metadata on all of the columns across all files in the RRFDir.
  */
class RRFCols(file: File, filename: String = "MRCOLS.RRF") extends RRFFile(file, filename) {
  /** Represents a single column entry. */
  case class Column(
    Name: String,
    Description: String,
    DocumentSectionNumber: String,
    MinimumLength: Option[Int],
    AverageLength: Option[Float],
    MaximumLength: Option[Int],
    Filename: String,
    SQL92DataType: String
  )

  /** Return a list of all columns in an RRFCols file. */
  def asColumns: Seq[Column] = {
    // We'll just hard-code this for now.
    // Eventually, it'd be nice to have this automatically settable from MRCOLS.RRF itself, but
    // right now I just don't have the time.
    cols.map(arr => Column(
      arr(0),
      arr(1),
      arr(2),
      arr(3).trim.toIntOption,
      arr(4).trim.toFloatOption,
      arr(5).trim.toIntOption,
      arr(6),
      arr(7)
    ))
  }
}

object RRFCols {
  def fromRRF(rrfFile: RRFFile) = new RRFCols(rrfFile.file, rrfFile.filename)
}