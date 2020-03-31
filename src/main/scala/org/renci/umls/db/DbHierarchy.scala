package org.renci.umls.db

import java.io.File
import java.sql.{Connection, PreparedStatement}

import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.apache.commons.dbcp2.ConnectionFactory
import org.renci.umls.rrf

import scala.util.Try
import org.renci.umls.rrf._

import scala.collection.mutable
import scala.io.Source

/** Represents a single hierarchy entry. */
case class HierarchyEntry(
                           ConceptId: String,                  // CUI
                           AtomId: String,                     // AUI
                           ContextNumber: String,              // CXN
                           ParentAtomId: String,               // PAUI
                           Source: String,                     // SAB
                           Relation: String,                   // RELA
                           PathToRoot: String,                 // PTR
                           HierarchyCode: String,              // HCD
                           ContentViewFlag: String             // CVF
                         )

/** A wrapper for RRFHierarchy that uses SQLite */
class DbHierarchy(db: ConnectionFactory, file: File, filename: String) extends RRFHierarchy(file, filename) with LazyLogging {
  /** The name of the table used to store this information. We include the SHA-256 hash so we reload it if it changes. */
  val tableName: String = "MRHIER_" + sha256

  /* Check to see if the MRHIER_ table seems up to date. If not, load it into memory from the file. */
  val conn1 = db.createConnection()
  val checkCount = conn1.createStatement()
  val results = Try { checkCount.executeQuery(s"SELECT COUNT(*) AS cnt FROM $tableName") }
  val rowsFromDb = if (results.isSuccess) results.get.getInt(1) else -1
  conn1.close()

  if (rowsFromDb > 0 && rowsFromDb == rowCount) {
    logger.info(s"Hierarchy table $tableName has $rowsFromDb rows.")
  } else {
    logger.info(s"Hierarchy table $tableName is not present or is out of sync. Regenerating.")

    val conn = db.createConnection()
    val regenerate = conn.createStatement()
    regenerate.execute(s"DROP TABLE IF EXISTS $tableName")
    regenerate.execute(s"""CREATE TABLE $tableName (
      |CUI TEXT,
      |AUI TEXT,
      |CXN TEXT,
      |PAUI TEXT,
      |SAB TEXT,
      |RELA TEXT,
      |PTR TEXT,
      |HCD TEXT,
      |CVF TEXT
      )""".stripMargin)

    val insertStmt = conn.prepareStatement(
      s"INSERT INTO $tableName (CUI, AUI, CXN, PAUI, SAB, RELA, PTR, HCD, CVF) " +
      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )

    var count = 0
    Source.fromFile(file).getLines.map(_.split("\\|", -1).toIndexedSeq) foreach { row =>
      insertStmt.clearParameters()

      (1 until 10) foreach ({ index =>
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
    regenerate.execute(s"CREATE INDEX INDEX_MRHIER_CUI ON $tableName (CUI);")
    regenerate.execute(s"CREATE INDEX INDEX_MRHIER_AUI ON $tableName (AUI);")
    regenerate.execute(s"CREATE INDEX INDEX_MRHIER_SAB ON $tableName (SAB);")

    conn.close()
  }

  override def getParents(atomIds: Seq[String]): Set[String] = {
    if (atomIds.isEmpty) return Set()

    val conn = db.createConnection()
    val questions = atomIds.map(_ => "?").mkString(", ")
    val query = conn.prepareStatement(s"SELECT PAUI FROM $tableName WHERE AUI IN ($questions)")
    val indexedSeq = atomIds.toIndexedSeq
    (1 to atomIds.size).foreach(index => {
      query.setString(index, indexedSeq(index - 1))
    })

    var results = Seq[String]()
    val rs = query.executeQuery()
    while(rs.next()) {
      results = rs.getString(1) +: results
    }
    conn.close()

    results.toSet
  }
}

object DbHierarchy {
  /** Wrap an RRF file using a database to cache results. */
  def fromDatabase(db: ConnectionFactory, rrfFile: RRFFile) = new DbHierarchy(db, rrfFile.file, rrfFile.filename)
}