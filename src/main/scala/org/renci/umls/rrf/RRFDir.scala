package org.renci.umls.rrf

import java.io.File

import scala.io.Source

/**
  * Wraps an entire directory of RRF files. This generally contains a release.dat file with release information and the
  * MRCOLS.RRF with information on all the columns across all the files.
  *
  * @param dir The META directory to wrap.
  */
class RRFDir(dir: File) {
  def getFile(filename: String): File = {
    val file = new File(dir, filename)

    if (!file.exists()) throw new RuntimeException(s"Directory ${dir.getCanonicalPath} does not contain expected file $filename.")

    file
  }
  def getRRFFile(filename: String): RRFFile = new RRFFile(getFile(filename), filename)

  /** Get the release information for this release (from release.dat) */
  val releaseInfo: String = Source.fromFile(getFile("release.dat")).mkString

  /** Loads the MRCOLS.RRF files and makes them available. */
  val cols: RRFCols = RRFCols.fromRRF(getRRFFile("MRCOLS.RRF"))
}
