package com.socrata.bq.query

import com.google.api.services.bigquery.model.{TableCell, TableRow, GetQueryResultsResponse}
import com.mchange.v2.c3p0.impl.NewProxyPreparedStatement
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.loader.sql.AbstractRepBasedDataSqlizer
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.datacoordinator.{id, Row, MutableRow}
import com.socrata.datacoordinator.util.CloseableIterator
import com.socrata.datacoordinator.id.{ColumnId, UserColumnId}
import com.socrata.bq.soql._
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.typed.ColumnRef
import com.socrata.soql.types.{SoQLType, SoQLPoint, SoQLText, SoQLValue}
import com.typesafe.scalalogging.slf4j.Logging
import java.sql.{SQLException, PreparedStatement, Connection, ResultSet}
import scala.collection.JavaConverters._

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

// TODO: Make this the main class for executing the query
class BBQRowReader[CT, CV] extends Logging {

  def query(analysis: SoQLAnalysis[UserColumnId, CT],
            toSql: (SoQLAnalysis[UserColumnId, CT], String) => BQSql,
            bqReps: OrderedMap[ColumnId, BBQReadRep[CT, CV]],
            querier: BBQQuerier,
            bqTableName: String) :
  CloseableIterator[com.socrata.datacoordinator.Row[CV]] with RowCount = {

    val bQSql = toSql(analysis, bqTableName)

    logger.debug(s"RAW QUERY $bQSql")

    val params = bQSql.setParams.toIterator
    val queryStr = bQSql.sql.toList.map(e => e.toString).map(s => if (s.equals("?") && params.hasNext) params.next else s).mkString

    logger.debug(s"QUERY: $queryStr")

    val decoders = bqReps.map { case (cid, rep) =>
      (cid, rep.numColumns, rep.SoQL(_))
    }.toArray

    // get rows
    if (analysis.selection.size > 0) {
      val bqResult = querier.query(queryStr)
      new BigQueryResultIt(bqResult, decodeBigQueryRow(decoders))
    } else {
      logger.debug("Queried a dataset with no user columns")
      EmptyIt
    }
  }

  def decodeBigQueryRow(decoders: Array[(ColumnId, Int, ((Seq[String]) => CV))])
                       (r: Seq[String]): com.socrata.datacoordinator.Row[CV] = {

    val row = new MutableRow[CV]
    var i = 0

    decoders.foreach { case (cid, numColumns, bqExtractor) =>
      row(cid) = bqExtractor(r.slice(i, i + numColumns))
      i += numColumns
    }
    row.freeze()
  }

  class BigQueryResultIt(pageIt: Iterator[GetQueryResultsResponse], toRow: (Seq[String] => Row[CV]))
    extends CloseableIterator[com.socrata.datacoordinator.Row[CV]] with RowCount {

    var rowCount: Option[Long] = None

    private var rowIt: Option[Iterator[TableRow]] = None

    override def next(): Row[CV] = {
      if (hasNext) {
        toRow(rowIt.get.next().getF.asScala.map(f => f.getV match {
          case s: String => s
          case _ => null
        }).toSeq)
      } else {
        throw new NoSuchElementException("No more data for the BigQueryResultSetIt")
      }
    }

    override def hasNext: Boolean = {
      if (!rowIt.exists(_.hasNext) && pageIt.hasNext) {
        val page = pageIt.next()
        logger.debug("Initializing row iterator")

        if (rowCount.isEmpty) {
          rowCount = Some(page.getTotalRows.longValue())
          logger.debug(s"Received ${rowCount.get} rows from BigQuery")
        }

        val rows = page.getRows
        if (rows != null) rowIt = Some(rows.iterator.asScala)
      }

      rowIt.exists(_.hasNext)
    }

    override def close(): Unit = {}
  }
}

object EmptyIt extends CloseableIterator[Nothing] with RowCount {
  val rowCount = Some(0L)
  def hasNext = false
  def next() = throw new NoSuchElementException("Called next() on an empty iterator")
  def close() {}
}

trait RowCount {

  def rowCount: Option[Long]

}
