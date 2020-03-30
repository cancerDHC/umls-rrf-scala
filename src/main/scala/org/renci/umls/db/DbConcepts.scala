package org.renci.umls.db

import java.io.File

import slick.jdbc.SQLiteProfile.backend.DatabaseDef

import org.renci.umls.rrf._

/** A wrapper for RRFConcepts that uses  */
class DbConcepts(db: DatabaseDef, file: File, filename: String) extends RRFConcepts(file, filename) {

}

object DbConcepts {
  /** Wrap an RRF file using a database to cache results. */
  def fromDatabase(db: DatabaseDef, rrfFile: RRFFile) = new DbConcepts(db, rrfFile.file, rrfFile.filename)
}