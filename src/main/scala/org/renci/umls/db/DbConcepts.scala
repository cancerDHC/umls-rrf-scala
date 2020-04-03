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

/** A wrapper for RRFConcepts that uses  */
class DbConcepts(db: ConnectionFactory, file: File, filename: String) extends RRFConcepts(file, filename) with LazyLogging {
  implicit val halfMapCache: Cache[Seq[HalfMap]] = CaffeineCache[Seq[HalfMap]]

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
    regenerate.execute(s"CREATE INDEX INDEX_MRCONSO_CODE ON $tableName (CODE);")

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

  def getHalfMapsForCodes(source: String, ids: Seq[String]): Seq[HalfMap] = memoizeSync(Some(2.seconds)) {
    // Retrieve all the fromIds.
    val conn = db.createConnection()
    if (ids.isEmpty) {
      val query = conn.prepareStatement(s"SELECT CUI, AUI, SAB, CODE, STR FROM $tableName WHERE SAB=?")
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
      logger.info(s"Loading halfmaps for $source with identifiers: $ids.")

      var halfMap = Seq[HalfMap]()
      var count = 0

      val windowSize = (ids.size/10) + 1
      ids.sliding(windowSize, windowSize).foreach(idGroup => {
        val indexedIds = idGroup.toIndexedSeq
        val questions = idGroup.map(_ => "?").mkString(", ")
        val query = conn.prepareStatement(s"SELECT DISTINCT CUI, AUI, SAB, CODE, STR FROM $tableName WHERE SAB=? AND CODE IN ($questions)")

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

  case class Mapping(
    fromSource: String,
    fromCode: String,
    toSource: String,
    toCode: String,
    conceptIds: Set[String],
    atomIds: Set[String],
    labels: Set[String]
  )
  def getMap(fromSource: String, fromIds: Seq[String], toSource: String, toIds: Seq[String]): Seq[Mapping] = {
    val fromHalfMaps = getHalfMapsForCodes(fromSource, fromIds)
    val toHalfMaps = getHalfMapsForCodes(toSource, toIds)

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
          Mapping(
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

  // Look up maps by CUIs.
  // TODO: we might want to be able to call this without source.
  def getMapsByCUIs(cuis: Seq[String], toSource: String): Seq[HalfMap] = memoizeSync(Some(2.seconds)) {
    if (cuis.isEmpty) return Seq()

    val conn = db.createConnection()
    val questions = cuis.map(_ => "?").mkString(", ")
    val query = conn.prepareStatement(s"SELECT DISTINCT CUI, AUI, SAB, CODE, STR FROM $tableName WHERE SAB=? AND CUI IN ($questions)")
    query.setString(1, toSource)
    val indexedSeq = cuis.toIndexedSeq
    (1 to cuis.size).foreach(index => {
      query.setString(index + 1, indexedSeq(index - 1))
    })

    var halfMaps = Seq[HalfMap]()
    val rs = query.executeQuery()
    while(rs.next()) {
      halfMaps = HalfMap(
        rs.getString(1),
        rs.getString(2),
        rs.getString(3),
        rs.getString(4),
        rs.getString(5)
      ) +: halfMaps
    }
    conn.close()

    halfMaps
  }

  // Get the CUIs for given AUIs.
  def getCUIsForAUI(auis: Seq[String]): Set[String] = {
    if (auis.isEmpty) return Set()

    val conn = db.createConnection()
    val questions = auis.map(_ => "?").mkString(", ")
    val query = conn.prepareStatement(s"SELECT DISTINCT CUI FROM $tableName WHERE AUI IN ($questions)")
    val indexedSeq = auis.toIndexedSeq
    (1 to auis.size).foreach(index => {
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

  def getAUIsForCUIs(cuis: Seq[String]): Seq[String] = {
    if (cuis.isEmpty) return Seq.empty

    val conn = db.createConnection()
    val questions = cuis.map(_ => "?").mkString(", ")
    val query = conn.prepareStatement(s"SELECT DISTINCT AUI FROM $tableName WHERE CUI IN ($questions)")
    val indexedSeq = cuis.toIndexedSeq
    (1 to cuis.size).foreach(index => {
      query.setString(index, indexedSeq(index - 1))
    })

    var results = Seq[String]()
    val rs = query.executeQuery()
    while(rs.next()) {
      results = rs.getString(1) +: results
    }
    conn.close()

    results
  }

  def getCUIsForCodes(source: String, ids: Seq[String]): Map[String, Seq[String]] = {
    if (ids.isEmpty) return Map.empty

    val conn = db.createConnection()
    val questions = ids.map(_ => "?").mkString(", ")
    val query = conn.prepareStatement(s"SELECT DISTINCT CODE, CUI FROM $tableName WHERE SAB=? AND CODE IN ($questions)")
    query.setString(1, source)
    val indexedSeq = ids.toIndexedSeq
    (1 to ids.size).foreach(index => {
      query.setString(index + 1, indexedSeq(index - 1))
    })

    var results = Seq[(String, String)]()
    val rs = query.executeQuery()
    while(rs.next()) {
      results = (rs.getString(1), rs.getString(2)) +: results
    }
    conn.close()

    results.groupMap(_._1)(_._2)
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