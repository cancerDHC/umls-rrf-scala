package org.renci.umls.db

import java.io.File
import java.sql.{Connection, PreparedStatement}

import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.apache.commons.dbcp2.ConnectionFactory
import org.renci.umls.rrf

import scala.util.Try
import org.renci.umls.rrf._
import scalacache._
import scalacache.caffeine._
import scalacache.memoization._
import scalacache.modes.sync._

import scala.concurrent.duration._
import scala.collection.mutable
import scala.io.Source

/** A wrapper for RRFMappings that uses  */
class DbMappings(db: ConnectionFactory, file: File, filename: String) extends RRFMappings(file, filename) with LazyLogging {
  /** The name of the table used to store this information. We include the SHA-256 hash so we reload it if it changes. */
  val tableName: String = "MRMAP_" + sha256

  /* Check to see if the MRMAP_ table seems up to date. If not, load it into memory from the file. */
  val conn1 = db.createConnection()
  val checkCount = conn1.createStatement()
  val results = Try { checkCount.executeQuery(s"SELECT COUNT(*) AS cnt FROM $tableName") }
  val rowsFromDb = if (results.isSuccess) results.get.getInt(1) else -1
  conn1.close()

  if (rowsFromDb > 0 && rowsFromDb == rowCount) {
    logger.info(s"Mappings table $tableName has $rowsFromDb rows.")
  } else {
    logger.info(s"Mappings table $tableName is not present or is out of sync. Regenerating.")

    val conn = db.createConnection()
    val regenerate = conn.createStatement()
    regenerate.execute(s"DROP TABLE IF EXISTS $tableName")
    regenerate.execute(s"""CREATE TABLE $tableName (
      |MAPSETCUI TEXT,
      |MAPSETSAB TEXT,
      |MAPSUBSETID TEXT,
      |MAPRANK TEXT,
      |MAPID TEXT,
      |MAPSID TEXT,
      |FROMID	TEXT,
      |FROMSID TEXT,
      |FROMEXPR	TEXT,
      |FROMTYPE	TEXT,
      |FROMRULE	TEXT,
      |FROMRES TEXT,
      |REL TEXT,
      |RELA TEXT,
      |TOID TEXT,
      |TOSID TEXT,
      |TOEXPR	TEXT,
      |TOTYPE	TEXT,
      |TORULE	TEXT,
      |TORES TEXT,
      |MAPRULE TEXT,
      |MAPRES TEXT,
      |MAPTYPE TEXT,
      |MAPATN	TEXT,
      |MAPATV	TEXT,
      |CVF TEXT
      )""".stripMargin)

    val insertStmt = conn.prepareStatement(
      s"INSERT INTO $tableName (MAPSETCUI, MAPSETSAB, MAPSUBSETID, MAPRANK, MAPID, MAPSID, FROMID, FROMSID, FROMEXPR, FROMTYPE, FROMRULE, FROMRES, REL, RELA, TOID, TOSID, TOEXPR, TOTYPE, TORULE, TORES, MAPRULE, MAPRES, MAPTYPE, MAPATN, MAPATV, CVF) " +
      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )

    var count = 0
    Source.fromFile(file).getLines.map(_.split("\\|", -1).toIndexedSeq) foreach { row =>
      insertStmt.clearParameters()

      (1 until 27) foreach ({ index =>
        insertStmt.setString(index, row(index - 1))
      })
      insertStmt.addBatch()

      count += 1
      if (count % 100000 == 0) {
        val percentage = count.toFloat/rowCount*100
        logger.info(f"Batched $count rows out of $rowCount ($percentage%.2f%%), executing.")
        insertStmt.executeBatch()
        insertStmt.clearBatch()
      }
    }
    insertStmt.executeBatch()

    // Add indexes.
    regenerate.execute(s"CREATE INDEX INDEX_MRMAP_FROMID ON $tableName (FROMID);")
    regenerate.execute(s"CREATE INDEX INDEX_MRMAP_TOID ON $tableName (TOID);")
    regenerate.execute(s"CREATE INDEX INDEX_MRMAP_REL ON $tableName (REL);")

    conn.close()
  }
}

object DbMappings {
  /** Wrap an RRF file using a database to cache results. */
  def fromDatabase(db: ConnectionFactory, rrfFile: RRFFile) = new DbMappings(db, rrfFile.file, rrfFile.filename)
}