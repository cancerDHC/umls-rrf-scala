package org.renci.umls.rrf

import java.io.File

import org.apache.commons.dbcp2.DriverManagerConnectionFactory
import org.renci.umls.db.{DbConcepts, DbHierarchy}

import scala.io.Source

/**
  * Wraps an entire directory of RRF files. This generally contains a release.dat file with release information and the
  * MRCOLS.RRF with information on all the columns across all the files.
  *
  * Some files, such as MRCONSO.RRF, are too large to reasonably work with directly. For those files, we maintain
  * a database in sqliteDbFile that these files are copied into. Queries then use the database rather than the file.
  *
  * @param dir The META directory to wrap.
  */
class RRFDir(dir: File, sqliteDbFile: File) {
  def getFile(filename: String): File = {
    val file = new File(dir, filename)

    if (!file.exists())
      throw new RuntimeException(
        s"Directory ${dir.getCanonicalPath} does not contain expected file $filename."
      )

    file
  }
  def getRRFFile(filename: String): RRFFile = new RRFFile(getFile(filename), filename)

  /** Set up an SQLite database for us to use. */
  lazy val sqliteDb: DriverManagerConnectionFactory = new DriverManagerConnectionFactory(
    "jdbc:sqlite:" + sqliteDbFile.getPath
  )

  /** Get the release information for this release (from release.dat) */
  lazy val releaseInfo: String = Source.fromFile(getFile("release.dat")).mkString
  val umlsVersionRegex = "(?m)^umls.release.name=(.*)\\s*$".r.unanchored
  lazy val releaseName: String = releaseInfo match {
    case umlsVersionRegex(version) => version
    case _                         => "unknown"
  }

  /** Loads MRCOLS.RRF files and makes them available. */
  lazy val cols: RRFCols = RRFCols.fromRRF(getRRFFile("MRCOLS.RRF"))

  /** Loads MRFILES.RRF files and makes them available. */
  lazy val files: RRFFiles = RRFFiles.fromRRF(getRRFFile("MRFILES.RRF"), cols)

  /** Loads MRHIER.RRF files and makes them available. */
  lazy val hierarchy: DbHierarchy = DbHierarchy.fromDatabase(sqliteDb, getRRFFile("MRHIER.RRF"))

  /** Loads MRCONSO.RRF files and makes them available. */
  lazy val concepts: DbConcepts = DbConcepts.fromDatabase(sqliteDb, getRRFFile("MRCONSO.RRF"))
}
