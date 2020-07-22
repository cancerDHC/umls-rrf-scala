package org.renci.umls.db

import java.io.File

import org.apache.commons.dbcp2.ConnectionFactory

import scala.util.Try
import org.renci.umls.rrf._

import scala.io.Source

/** Represents a single hierarchy entry. */
case class Relationship(
  ConceptId1: String, // CUI1
  AtomId1: String, // AUI1
  SourceType1: String, // STYPE1
  Relationship: String, // REL
  ConceptId2: String, // CUI2
  AtomId2: String, // AUI2
  SourceType2: String, // STYPE2
  SpecificRelationship: String, // RELA
  RelationshipId: String, // RUI
  SourceRelationshipId: String, // SRUI
  AbbrSourceName: String, // SAB
  RelationshipLabelSource: String, // SL
  RelationshipGroup: String, // RG
  SourceAssertedDirectionality: String, // DIR
  Suppressible: String, // SUPPRESS
  ContentViewFlag: String // CVF
)

case class Relation(
  cui1: String,
  label1: String,
  aui1: String,
  rel: String,
  rela: String,
  direction: String,
  cui2: String,
  label2: String,
  aui2: String
)

/** A wrapper for RRFRelationships that uses SQLite */
class DbRelationships(db: ConnectionFactory, file: File, filename: String, dbConcepts: DbConcepts)
    extends RRFHierarchy(file, filename) {

  /** The name of the table used to store this information. We include the SHA-256 hash so we reload it if it changes. */
  val tableName: String = "MRREL_" + sha256

  /* Check to see if the MRREL_ table seems up to date. If not, load it into memory from the file. */
  val conn1 = db.createConnection()
  val checkCount = conn1.createStatement()
  val results = Try { checkCount.executeQuery(s"SELECT COUNT(*) AS cnt FROM $tableName") }
  val rowsFromDb = if (results.isSuccess) results.get.getInt(1) else -1
  conn1.close()

  if (rowsFromDb > 0 && rowsFromDb == rowCount) {
    scribe.info(s"Relationships table $tableName has $rowsFromDb rows.")
  } else {
    scribe.info(s"Relationships table $tableName is not present or is out of sync. Regenerating.")

    val conn = db.createConnection()
    val regenerate = conn.createStatement()
    regenerate.execute(s"DROP TABLE IF EXISTS $tableName")
    regenerate.execute(s"""CREATE TABLE $tableName (
      |CUI1 TEXT,
      |AUI1 TEXT,
      |STYPE1 TEXT,
      |REL TEXT,
      |CUI2 TEXT,
      |AUI2 TEXT,
      |STYPE2 TEXT,
      |RELA TEXT,
      |RUI TEXT,
      |SRUI TEXT,
      |SAB TEXT,
      |SL TEXT,
      |RG TEXT,
      |DIR TEXT,
      |SUPPRESS TEXT,
      |CVF TEXT
      )""".stripMargin)

    val insertStmt = conn.prepareStatement(
      s"INSERT INTO $tableName (CUI1, AUI1, STYPE1, REL, CUI2, AUI2, STYPE2, RELA, RUI, SRUI, SAB, SL, RG, DIR, SUPPRESS, CVF) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )

    var count = 0
    Source.fromFile(file).getLines.map(_.split("\\|", -1).toIndexedSeq) foreach { row =>
      insertStmt.clearParameters()

      (1 until 17) foreach ({ index =>
        insertStmt.setString(index, row(index - 1))
      })
      insertStmt.addBatch()

      count += 1
      if (count % 100000 == 0) {
        val percentage = count.toFloat / rowCount * 100
        scribe.info(f"Batched $count rows out of $rowCount ($percentage%.2f%%), executing.")
        insertStmt.executeBatch()
        insertStmt.clearBatch()
      }
    }
    insertStmt.executeBatch()

    // Add indexes.
    regenerate.execute(s"CREATE INDEX INDEX_MRREL_CUI1 ON $tableName (CUI1);")
    regenerate.execute(s"CREATE INDEX INDEX_MRREL_CUI2 ON $tableName (CUI2);")
    regenerate.execute(s"CREATE INDEX INDEX_MRREL_AUI1 ON $tableName (AUI1);")
    regenerate.execute(s"CREATE INDEX INDEX_MRREL_AUI2 ON $tableName (AUI2);")
    regenerate.execute(s"CREATE INDEX INDEX_MRREL_REL ON $tableName (REL);")
    regenerate.execute(s"CREATE INDEX INDEX_MRREL_RELA ON $tableName (RELA);")

    conn.close()
  }

  def getRelationshipsByCUIs(cuis: Set[String]): Seq[Relation] = {
    if (cuis.isEmpty) return Seq.empty

    val mrconso_tablename = dbConcepts.tableName
    val conn = db.createConnection()
    val questions = cuis.toSeq.map(_ => "?").mkString(", ")
    val query =
      conn.prepareStatement(s"""
      SELECT DISTINCT CUI1, mrconso1.STR AS label1, AUI1, REL, RELA, DIR, CUI2, mrconso2.STR AS label2, AUI2
      FROM $tableName
        LEFT JOIN ${mrconso_tablename} AS mrconso1 ON CUI1=mrconso1.CUI
        LEFT JOIN ${mrconso_tablename} AS mrconso2 ON CUI2=mrconso2.CUI
      WHERE CUI1 IN ($questions)
      ;""")
    val indexedSeq = cuis.toIndexedSeq
    scribe.info(s"Prepared questions ${questions} for CUIs: ${cuis}")
    (1 to cuis.size).foreach(index => {
      query.setString(index, indexedSeq(index - 1))
    })

    var results = Seq[Relation]()
    val rs = query.executeQuery()
    while (rs.next()) {
      results = Relation(
        rs.getString(1),
        rs.getString(2),
        rs.getString(3),
        rs.getString(4),
        rs.getString(5),
        rs.getString(6),
        rs.getString(7),
        rs.getString(8),
        rs.getString(9)
      ) +: results
    }
    conn.close()

    results
  }

  def getEitherRelationshipsByCUIs(cuis: Set[String]): Seq[Relation] = {
    if (cuis.isEmpty) return Seq.empty

    val mrconso_tablename = dbConcepts.tableName
    val conn = db.createConnection()
    val questions = cuis.toSeq.map(_ => "?").mkString(", ")
    val query =
      conn.prepareStatement(s"""
      SELECT DISTINCT CUI1, mrconso1.STR AS label1, AUI1, REL, RELA, DIR, CUI2, mrconso2.STR AS label2, AUI2
      FROM $tableName
        LEFT JOIN ${mrconso_tablename} AS mrconso1 ON CUI1=mrconso1.CUI
        LEFT JOIN ${mrconso_tablename} AS mrconso2 ON CUI2=mrconso2.CUI
      WHERE CUI1 IN ($questions) OR CUI2 IN ($questions)
      ;""")
    val indexedSeq = cuis.toIndexedSeq
    scribe.info(s"Prepared questions ${questions} for CUIs: ${cuis}")
    (1 to cuis.size).foreach(index => {
      query.setString(index, indexedSeq(index - 1))
      query.setString(index + cuis.size, indexedSeq(index - 1))
    })

    var results = Seq[Relation]()
    val rs = query.executeQuery()
    while (rs.next()) {
      results = Relation(
        rs.getString(1),
        rs.getString(2),
        rs.getString(3),
        rs.getString(4),
        rs.getString(5),
        rs.getString(6),
        rs.getString(7),
        rs.getString(8),
        rs.getString(9)
      ) +: results
    }
    conn.close()

    results
  }
}

object DbRelationships {

  /** Wrap an RRF file using a database to cache results. */
  def fromDatabase(db: ConnectionFactory, rrfFile: RRFFile, dbConcepts: DbConcepts) =
    new DbRelationships(db, rrfFile.file, rrfFile.filename, dbConcepts: DbConcepts)
}
