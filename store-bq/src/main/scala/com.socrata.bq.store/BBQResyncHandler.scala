package com.socrata.bq.store

import com.socrata.bq.soql.BigQueryRepFactory

import com.rojoma.json.v3.ast._
import com.rojoma.simplearm.Managed
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.soql.types._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.secondary.{CopyInfo => SecondaryCopyInfo, ColumnInfo => SecondaryColumnInfo, _}
import com.socrata.datacoordinator.id.{DatasetId, CopyId, ColumnId, UserColumnId}
import com.typesafe.scalalogging.slf4j.Logging

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.ByteArrayContent
import com.google.api.services.bigquery.Bigquery
import com.google.api.services.bigquery.model._

class BBQResyncHandler(bqProjectId: String, bqDatasetId: String) extends Logging {

  private def parseDatasetId(datasetInternalName: String) = {
    datasetInternalName.split('.')(1).toInt
  }

  def handle(bigquery: Bigquery,
             datasetInfo: DatasetInfo,
             copyInfo: SecondaryCopyInfo,
             schema: ColumnIdMap[SecondaryColumnInfo[SoQLType]],
             managedRowIterator: Managed[Iterator[ColumnIdMap[SoQLValue]]]) : Unit = {
    logger.info(s"resyncing ${datasetInfo.internalName}")
    val datasetId = parseDatasetId(datasetInfo.internalName)
    // make table reference and bigquery metadata
    val columnNames: ColumnIdMap[String] = BigqueryUtils.makeColumnNameMap(schema)
    val ref = BigqueryUtils.makeTableReference(bqProjectId, bqDatasetId, datasetInfo, copyInfo)
    val bqSchema = BigqueryUtils.makeTableSchema(schema, columnNames)
    val table = new Table()
            .setTableReference(ref)
            .setSchema(bqSchema)
    // try to create the table
    try {
      bigquery.tables.insert(bqProjectId, bqDatasetId, table).execute()
      logger.info(s"Inserting into ${ref.getTableId}")
    } catch {
      case e: GoogleJsonResponseException => {
        if (e.getDetails.getCode == 409) {
          // Table already exists. Replace it.
          bigquery.tables.update(bqProjectId, bqDatasetId, ref.getTableId, table).execute()
        } else {
          throw e
        }
      }
    }

    var truncate = true
    for { rowIterator <- managedRowIterator } {
      val requests =
        for {
          row: ColumnIdMap[SoQLValue] <- rowIterator
        } yield {
          val rowMap = row.foldLeft(Map[String, JValue]()) { case (map, (id, value)) =>
            columnNames.get(id) match {
              case None => map
              case Some(name) => map + ((name, BigQueryRepFactory(value.typ).jvalue(value)))
            }
          }
          JsonUtil.renderJson(rowMap)
        }

      for { batch <- requests.grouped(10000) } {
        val content = new ByteArrayContent("application/octet-stream", batch.mkString("\n").toCharArray.map(_.toByte))
        val job = BigqueryUtils.makeLoadJob(ref, truncate)
        truncate = false
        val insert = bigquery.jobs.insert(bqProjectId, job, content)
        insert.execute()
      }
    }
  }
}
