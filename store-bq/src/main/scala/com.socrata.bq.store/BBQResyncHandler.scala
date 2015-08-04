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
import com.google.api.services.bigquery.{Bigquery, BigqueryRequest}
import com.google.api.services.bigquery.model._

object BBQResyncHandler {
  private var die = true
  private def shouldDie: Boolean = {
    if (die) {
      die = false
      true
    } else {
      false
    }
  }
}

class BBQResyncHandler(bigquery: Bigquery, bqProjectId: String, bqDatasetId: String) extends Logging {

  class BBQJob(job: Job) {
    val jobId = job.getJobReference.getJobId
    var state = job.getStatus.getState
    logger.debug(s"new bigquery job ${jobId} with state ${state}")

    def checkStatus: String = {
      val job = bigquery.jobs.get(bqProjectId, jobId).setFields("status").execute()
      val newState = job.getStatus.getState
      if (newState != state) {
        logger.debug(s"new state for job ${jobId}: ${newState}")
        state = newState
      }
      if (!List("PENDING", "RUNNING", "DONE").contains(state)) {
        logger.info(s"unexpected job state: ${state}")
      }
      state
    }
  }

  private def parseDatasetId(datasetInternalName: String) = {
    datasetInternalName.split('.')(1).toInt
  }

  private def executeAndAcceptResponseCodes[T](request: BigqueryRequest[T], acceptableResponseCodes: Int*) : Option[T] = {
    try {
      Some(request.execute())
    } catch {
      case e: GoogleJsonResponseException if acceptableResponseCodes contains e.getDetails.getCode => None
    }
  }

  def handle(datasetInfo: DatasetInfo,
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
    // delete the table, just in case it already exists (from a failed resync, perhaps)
    val delete = bigquery.tables.delete(bqProjectId, bqDatasetId, ref.getTableId)
    executeAndAcceptResponseCodes(delete, 404)
    // create the table
    val insert = bigquery.tables.insert(bqProjectId, bqDatasetId, table)
    executeAndAcceptResponseCodes(insert, 400)
    logger.info(s"Inserting into ${ref.getTableId}")

    for { rowIterator <- managedRowIterator } {
      val rows =
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

      val jobIterator =
        for {
          batch <- rows.grouped(10000)
        } yield {
          val buffer = batch.mkString("\n").toCharArray.map(_.toByte)
          buffer.update(buffer.length-1, 100) // corrupt the buffer
          val content = new ByteArrayContent("application/octet-stream", buffer)
          val job = BigqueryUtils.makeLoadJob(ref)
          val insert = bigquery.jobs.insert(bqProjectId, job, content).setFields("jobReference,status")
          val jobResponse = insert.execute()
          new BBQJob(jobResponse)
        }

      // force jobs to execute
      var jobs = jobIterator.toList

      while (jobs.length > 0) {
        // does this thread sleep block other workers?
        Thread.sleep(1000 /* 1s */)
        jobs = jobs.filter { job =>
          job.checkStatus != "DONE"
        }
      }
      logger.debug("done with all jobs")
    }
  }
}
