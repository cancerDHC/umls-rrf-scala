package org.renci.sssom

/**
  * A RowFiller can fill in a row in SSSOM format. It returns a copy of the row that was provided,
  * but possibly with additional columns filled in.
  *
  * There are no guarantees as to the order in which the fields are returned; however, column names
  * not provided in the input file may not be written into the output file.
  */
abstract class SSSOMFiller {
  /**
    * Fill in the input row. The list of all headers is also provided.
    * @return None if this row could not be filled, and Some[Row] if it can.
    */
  def fillRow(row: SSSOMFiller.Row, headers: List[String]): Option[Seq[SSSOMFiller.Result]]
}

object SSSOMFiller {
  /** A row is a map of column names to their values in this row. */
  type Row = Map[String, String]

  /** Wraps a result of a row filler operation. */
  case class Result(
    source: Row,
    result: Row,
    filler: SSSOMFiller
  )
}