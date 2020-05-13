package org.renci.umls.rrf

import java.io.File

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

/**
  * The RRFCols file contains metadata on all of the columns across all files in the RRFDir.
  */
class RRFCols(file: File, filename: String = "MRCOLS.RRF") extends RRFFile(file, filename) {
  /** A list of all columns in an RRFCols file. */
  val columns: Seq[Column] = {
    // We'll just hard-code this for now.
    // Eventually, it'd be nice to have this automatically settable from MRCOLS.RRF itself, but
    // right now I just don't have the time.
    rows.map(
      arr =>
        Column(
          arr(0),
          arr(1),
          arr(2),
          arr(3).trim.toIntOption,
          arr(4).trim.toFloatOption,
          arr(5).trim.toIntOption,
          arr(6),
          arr(7)
        )
    )
  }

  /** Retrieve a column by name. */
  def getColumn(name: String, filename: String): Seq[Column] =
    columns.filter(_.Filename == filename).filter(_.Name == name)
  def getOnlyColumn(name: String, filename: String): Column = {
    val results = getColumn(name, filename)
    if (results.size < 1)
      throw new RuntimeException(
        s"No column named $name found for filename $filename in ${this.filename}"
      )
    else if (results.size > 1)
      throw new RuntimeException(
        s"Too many columns named $name found for filename $filename in ${this.filename}: $results"
      )
    else results.head
  }
}

object RRFCols {
  /** Wrap an RRF file as an RRFCols. */
  def fromRRF(rrfFile: RRFFile) = new RRFCols(rrfFile.file, rrfFile.filename)
}
