package com.socrata.bq.query

import com.google.api.services.bigquery.model.{TableRow, GetQueryResultsResponse}
import com.socrata.datacoordinator.{Row, MutableRow}
import com.socrata.datacoordinator.util.CloseableIterator
import com.socrata.datacoordinator.id.{ColumnId, UserColumnId}
import com.socrata.bq.soql._
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.SoQLAnalysis
import com.typesafe.scalalogging.slf4j.Logging
import scala.collection.JavaConverters._

class BBQRowReader[CT, CV] extends Logging {

  /**
   * Execute a query against BigQuery and return the results and row count
   * @param analysis parsed soql
   * @param toSql function to convert the analysis to BQSql
   * @param bqReps a map from column id to conversion rep for each column selected in the query
   * @param querier connection to BigQuery for issuing the query
   * @param bqTableName the name of the table in BigQuery
   * @return an iterator over the rows of the result and a row count
   */
  def query(analysis: SoQLAnalysis[UserColumnId, CT],
            toSql: (SoQLAnalysis[UserColumnId, CT], String) => BQSql,
            bqReps: OrderedMap[ColumnId, BBQReadRep[CT, CV]],
            querier: BBQQuerier,
            bqTableName: String) :
  CloseableIterator[com.socrata.datacoordinator.Row[CV]] with RowCount = {

    val bQSql = toSql(analysis, bqTableName)
    val queryStr = bQSql.injectParams

    logger.debug(s"Raw query: $bQSql")
    logger.debug(s"Query: $queryStr")

    // Grab the SoQL conversion function for each selected column's rep
    val decoders = bqReps.map { case (cid, rep) =>
      (cid, rep.numColumns, rep.SoQL(_))
    }.toArray

    // If the query contains a selection, issue the query and return an iterator over the resulting rows, otherwise
    // return an empty iterator
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
