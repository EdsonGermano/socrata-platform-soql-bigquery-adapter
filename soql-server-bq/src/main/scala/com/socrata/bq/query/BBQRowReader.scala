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
   * @param querier connection to BigQuery for executing the query
   * @param bqTableName table name in Bigquery in the form '[dataset-id.table-name]'
   * @return an iterator over the result of the query and an exact row count
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

    // For each selected column's rep, grab the `soql` function and the `numColumns` consumed
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

  /**
   * Convert data in a row using the series of provided 'decoder' functions
   * @param decoders array of Tuple3[column id, number of columns to read, decoder function]
   * @param originalRow row to process
   */
  def decodeBigQueryRow(decoders: Array[(ColumnId, Int, ((Seq[String]) => CV))])
                       (originalRow: Seq[String]): com.socrata.datacoordinator.Row[CV] = {

    val decodedRow = new MutableRow[CV]
    var i = 0

    // Iterate over the column reps and process as many columns from the row as that rep requires,
    // moving forward in the row as columns are read
    decoders.foreach { case (cid, numColumns, bqExtractor) =>
      decodedRow(cid) = bqExtractor(originalRow.slice(i, i + numColumns))
      i += numColumns
    }
    decodedRow.freeze()
  }

  /**
   * Iterator over rows returned by Bigquery
   * @param pageIt iterator over pages in the result
   * @param toRow Bigquery row conversion method
   */
  class BigQueryResultIt(pageIt: Iterator[GetQueryResultsResponse], toRow: (Seq[String] => Row[CV]))
    extends CloseableIterator[com.socrata.datacoordinator.Row[CV]] with RowCount {

    var rowCount: Option[Long] = None
    private var rowIt: Option[Iterator[TableRow]] = None

    override def next(): Row[CV] = {
      if (hasNext) {
        // The value returned is either a Java.lang.String containing the data value, or Java.lang.Object if the
        // value is null, so we must match on the value's type to determine whether we receive a null value:
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

        // The exact row count is accessible in each value of the page iterator, so grab it and save it the
        // first time we increment the page iterator
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

/**
 * An exact row count for a query result
 */
trait RowCount {
  def rowCount: Option[Long]
}
