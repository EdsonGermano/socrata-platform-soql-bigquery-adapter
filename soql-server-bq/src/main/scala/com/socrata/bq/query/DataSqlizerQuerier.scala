package com.socrata.bq.query

import com.mchange.v2.c3p0.impl.NewProxyPreparedStatement
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.loader.sql.AbstractRepBasedDataSqlizer
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.datacoordinator.{id, Row, MutableRow}
import com.socrata.datacoordinator.util.CloseableIterator
import com.socrata.datacoordinator.id.{ColumnId, UserColumnId}
import com.socrata.bq.soql.{Escape, SoQLBigQueryReadRep, BigQueryRepFactory, BQSql}
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.typed.ColumnRef
import com.socrata.soql.types.{SoQLText, SoQLValue}
import com.typesafe.scalalogging.slf4j.Logging
import java.sql.{SQLException, PreparedStatement, Connection, ResultSet}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

trait DataSqlizerQuerier[CT, CV] extends AbstractRepBasedDataSqlizer[CT, CV] with Logging {
  this: AbstractRepBasedDataSqlizer[CT, CV] =>

  val PROJECT_NAME = "thematic-bee-98521"
  var TABLE_NAME = "[ids.nyc]"

  def query(conn: Connection, analysis: SoQLAnalysis[UserColumnId, CT],
               toSql: (SoQLAnalysis[UserColumnId, CT], String) => BQSql, // analsysis, tableName
               toRowCountSql: (SoQLAnalysis[UserColumnId, CT], String) => BQSql, // analsysis, tableName
               reqRowCount: Boolean,
               querySchema: OrderedMap[ColumnId, SqlColumnRep[CT, CV]],
               bqReps: OrderedMap[ColumnId, SoQLBigQueryReadRep[CT, CV]]) :
               CloseableIterator[com.socrata.datacoordinator.Row[CV]] with RowCount = {

    // For some weird reason, when you iterate over the querySchema, a new Rep is created from scratch
    // every time, which is very expensive.  Move that out of the inner loop of decodeRow.
    // Also, the orderedMap is extremely inefficient and very complex to debug.
    // TODO: refactor PG server not to use Ordered Map.
//    val decoders2 = querySchema.map { case (cid, rep) =>
//      (cid, rep.fromResultSet(_, _), rep.physColumns.length)
//    }.toArray
    val bQSql = toSql(analysis, TABLE_NAME)

    logger.debug(s"RAW QUERY $bQSql")

    val params = bQSql.setParams.toIterator
    val queryStr = bQSql.sql.toList.map(e => e.toString).map(s => if (s.equals("?")) params.next else s).mkString

    logger.debug(s"QUERY: $queryStr")


//    val decoders = Array(Tuple2(new ColumnId(1), bqReps(0).SoQL(_)))
    val decoders = bqReps.map { case (cid, rep) =>
      (cid, rep.SoQL(_))
    }.toArray

    // get rows
    logger.debug("right before analysis selection")
    if (analysis.selection.size > 0) {
      logger.debug("right before bigquery query method")
      val bqResult = BigQueryQuerier.query(PROJECT_NAME, queryStr)
      logger.debug("right after query method")
      if (bqResult != null) {
        logger.debug("Received " + bqResult.rowCount + " rows from BigQuery")
        new BigQueryResultIt(Option(bqResult.rowCount), bqResult, decodeBigQueryRow(decoders))
      } else {
        logger.debug("Queried a dataset with no user columns")
      }
    }
    EmptyIt
  }

  def decodeBigQueryRow(decoders: Array[(ColumnId, String => CV)])
                       (m : mutable.Buffer[String]): com.socrata.datacoordinator.Row[CV] = {

    val row = new MutableRow[CV]
    var i = 0

    decoders.foreach { case (cid, bqExtractor) =>
        row(cid) = bqExtractor(m(i))
        i += 1
    }
    row.freeze()
  }

  class BigQueryResultIt(val rowCount : Option[Long], rows: ArrayBuffer[mutable.Buffer[String]], toRow: (mutable
  .Buffer[String] => Row[CV]))
    extends CloseableIterator[com.socrata.datacoordinator.Row[CV]] with RowCount {

    private val it: Iterator[mutable.Buffer[String]] = rows.iterator

    override def next(): Row[CV] = {
      if (hasNext) {
        val rowVal = it.next()
        logger.info("BQ row value: " + rowVal)
        toRow(rowVal)
      } else {
        throw new Exception("No more data for the BigQueryResultSetIt")
      }
    }

    override def hasNext: Boolean = {
      it.hasNext
    }

    override def close(): Unit = {}
  }

  object EmptyIt extends CloseableIterator[Nothing] with RowCount {
    val rowCount = Some(0L)
    def hasNext = false
    def next() = throw new Exception("Called next() on an empty iterator")
    def close() {}
  }
}

trait RowCount {

  val rowCount: Option[Long]

}
