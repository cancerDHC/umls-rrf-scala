package org.renci.umls.rrf

import java.io.File

/** Represents a single file entry. */
case class FileEntry(
  Name: String,
  Description: String,
  ColumnsAsString: String,
  Columns: Array[Column],
  NumberOfColumns: Int,
  NumberOfRows: Option[Long],
  SizeInBytes: Option[Long]
)

/**
  * The RRFFiles file contains metadata on all of the files in the RRFDir. This is essential, since this contains a
  * list of all the columns in the file.
  */
class RRFFiles(file: File, cols: RRFCols, filename: String = "MRFILES.RRF") extends RRFFile(file, filename) {
  /** Return a list of all files in an RRFFiles file. */
  def files: Seq[FileEntry] = {
    // We'll just hard-code this for now.
    // Eventually, it'd be nice to have this automatically settable from MRCOLS.RRF itself, but
    // right now I just don't have the time.
    rows.map(arr => {
      val filename = arr(0)
      val columns = arr(2).split("\\s*,\\s*").map(cols.getOnlyColumn(_, filename))
      val colSize = arr(3).trim.toInt

      assert(
        columns.size == colSize,
        s"Number of columns in the string ('${arr(2)}') should equal the number in the column (${arr(4)})"
      )

      FileEntry(
        filename,
        arr(1),
        arr(2),
        columns,
        colSize,
        arr(4).trim.toLongOption,
        arr(5).trim.toLongOption
      )
    })
  }
}

object RRFFiles {
  /** Wrap an RRF file as an RRFFiles class. */
  def fromRRF(rrfFile: RRFFile, rrfCols: RRFCols) = new RRFFiles(rrfFile.file, rrfCols, rrfFile.filename)
}