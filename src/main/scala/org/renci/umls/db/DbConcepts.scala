package org.renci.umls.db

import java.io.File
import java.sql.{Connection, PreparedStatement}

import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.apache.commons.dbcp2.ConnectionFactory

import scala.util.Try
import org.renci.umls.rrf._

import scala.io.Source

/** A wrapper for RRFConcepts that uses  */
class DbConcepts(db: ConnectionFactory, file: File, filename: String) extends RRFConcepts(file, filename) with LazyLogging {
  val tableName: String = "MRCONSO_" + sha256

  val conn1 = db.createConnection()
  val checkCount = conn1.createStatement()
  val results = Try { checkCount.executeQuery(s"SELECT COUNT(*) AS cnt FROM $tableName") }
  val rowsFromDb = if (results.isSuccess) results.get.getInt(1) else -1
  conn1.close()

  if (rowsFromDb > 0 && rowsFromDb == rowCount) {
    logger.info(s"Concept table $tableName has $rowsFromDb rows.")
  } else {
    logger.info(s"Concept table $tableName is not present or is out of sync. Regenerating.")

    val conn = db.createConnection()
    val regenerate = conn.createStatement()
    regenerate.execute(s"DROP TABLE IF EXISTS $tableName")
    regenerate.execute(s"""CREATE TABLE $tableName (
      |CUI TEXT,
      |LAT TEXT,
      |TS TEXT,
      |LUI TEXT,
      |STT TEXT,
      |SUI TEXT,
      |ISPREF TEXT,
      |AUI TEXT,
      |SAUI TEXT,
      |SCUI TEXT,
      |SDUI TEXT,
      |SAB TEXT,
      |TTY TEXT,
      |CODE TEXT,
      |STR TEXT,
      |SRL TEXT,
      |SUPPRESS TEXT,
      |CVF TEXT
      )""".stripMargin)

    val insertStmt = conn.prepareStatement(
      s"INSERT INTO $tableName (CUI, LAT, TS, LUI, STT, SUI, ISPREF, AUI, SAUI, SCUI, SDUI, SAB, TTY, CODE, STR, SRL, SUPPRESS, CVF) " +
      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )

    var count = 0
    Source.fromFile(file).getLines.map(_.split("\\|", -1).toIndexedSeq) foreach { row =>
      insertStmt.clearParameters()

      (1 until 19) foreach ({ index =>
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
    conn.close()
  }
}

/** Represents a single column entry. */
case class Concept(
                    ConceptID: String,            // CUI
                    Lang: String,                 // LAT
                    TermStatus: String,           // TS
                    TermID: String,               // LUI
                    StringType: String,           // STT
                    StringID: String,             // SUI
                    IsPreferred: Boolean,         // ISPREF
                    AtomID: String,               // AUI
                    SourceAtomID: String,         // SAUI
                    SourceConceptID: String,      // SCUI
                    SourceDescriptorID: String,   // SDUI
                    Source: String,               // SAB
                    TermType: String,             // TTY
                    SourceEntryID: String,        // CODE
                    EntryString: String,          // STR
                    SourceRestriction: String,    // SRL
                    SuppressibleFlag: String,     // SUPPRESS
                    ContentViewFlag: String       // CVF
                  )

object DbConcepts {
  /** Wrap an RRF file using a database to cache results. */
  def fromDatabase(db: ConnectionFactory, rrfFile: RRFFile) = new DbConcepts(db, rrfFile.file, rrfFile.filename)
}