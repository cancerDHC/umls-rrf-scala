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

/** A wrapper for RRFConcepts that uses  */
class DbConcepts(db: ConnectionFactory, file: File, filename: String) extends RRFConcepts(file, filename) with LazyLogging {
  /** The name of the table used to store this information. We include the SHA-256 hash so we reload it if it changes. */
  val tableName: String = "MRCONSO_" + sha256

  /* Check to see if the MRCONSO_ table seems up to date. If not, load it into memory from the file. */
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

    // Add indexes.
    regenerate.execute(s"CREATE INDEX INDEX_MRCONSO_SAB ON $tableName (SAB);")

    conn.close()
  }

  // Okay, we're ready to go!
  def getSources(): Seq[(String, Int)] = {
    val conn = db.createConnection()
    val query = conn.createStatement()
    val rs = query.executeQuery(s"SELECT SAB, COUNT(*) AS count FROM $tableName GROUP BY SAB ORDER BY count DESC;")

    var results = Seq[(String, Int)]()
    while(rs.next()) {
      results = results :+ (
        rs.getString(1),
        rs.getInt(2)
      )
    }

    conn.close()
    results
  }

  // We use the CUIs to map everything from the fromSource to the toSource.
  case class HalfMap(cui: String, aui: String, source: String, code:String, label:String)

  def getHalfMaps(source: String, ids: Seq[String]): Seq[HalfMap] = {
    // Retrieve all the fromIds.
    val conn = db.createConnection()
    if (ids.isEmpty) {
      val query = conn.prepareStatement(s"SELECT CUI, AUI, SAB, SCUI, STR FROM $tableName WHERE SAB=?")
      query.setString(1, source)
      val rs = query.executeQuery()

      logger.info(s"Loading halfmaps for $source")
      var halfMap = Seq[HalfMap]()
      var count = 0
      while(rs.next()) {
        halfMap = HalfMap(
          rs.getString(1),
          rs.getString(2),
          rs.getString(3),
          rs.getString(4),
          rs.getString(5)
        ) +: halfMap
        count += 1
        if (count % 100000 == 0) {
          logger.info(s"Loaded $count halfmaps.")
        }
      }

      conn.close()
      logger.info(s"${halfMap.size} halfmaps loaded.")

      halfMap
    } else {
      logger.info(s"Loading halfmaps for $source with ${ids.size} identifiers.")

      var halfMap = Seq[HalfMap]()
      var count = 0

      val windowSize = (ids.size/20)
      ids.sliding(windowSize, windowSize).foreach(idGroup => {
        val indexedIds = idGroup.toIndexedSeq
        val questions = idGroup.map(_ => "?").mkString(", ")
        val query = conn.prepareStatement(s"SELECT CUI, AUI, SAB, SCUI, STR FROM $tableName WHERE SAB=? AND SCUI IN ($questions)")

        query.setString(1, source)
        (0 until idGroup.size).foreach(id => {
          query.setString(id + 2, indexedIds(id))
        })

        val rs = query.executeQuery()
        while(rs.next()) {
          halfMap = HalfMap(
            rs.getString(1),
            rs.getString(2),
            rs.getString(3),
            rs.getString(4),
            rs.getString(5)
          ) +: halfMap
          count += 1
        }

        logger.info(s"Loaded $count halfmaps.")
      })

      conn.close()
      logger.info(s"${halfMap.size} halfmaps loaded.")

      halfMap
    }
  }

  case class Map(
    fromSource: String,
    fromCode: String,
    toSource: String,
    toCode: String,
    conceptIds: Set[String],
    atomIds: Set[String],
    labels: Set[String]
  )
  def getMap(fromSource: String, fromIds: Seq[String], toSource: String, toIds: Seq[String]): Seq[Map] = {
    val fromHalfMaps = getHalfMaps(fromSource, fromIds)
    val toHalfMaps = getHalfMaps(toSource, toIds)

    // Combine the halfmaps so we need to.
    (fromHalfMaps ++ toHalfMaps).groupBy(_.cui).values.flatMap({ entries =>
      // Everything in entries is the "same" concept according to MRCONSO.
      // So we partition this based on
      val cuis = entries.map(_.cui).toSet
      val auis = entries.map(_.aui).toSet
      val labels = entries.map(_.label).toSet
      val fromCodes = entries.filter(_.source == fromSource).map(_.code).toSet[String]
      val toCodes = entries.filter(_.source == toSource).map(_.code).toSet[String]

      fromCodes.flatMap(fromCode => {
        toCodes.map(toCode => {
          Map(
            fromSource,
            fromCode,
            toSource,
            toCode,
            cuis,
            auis,
            labels
          )
        })
      })
    }).toSeq
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