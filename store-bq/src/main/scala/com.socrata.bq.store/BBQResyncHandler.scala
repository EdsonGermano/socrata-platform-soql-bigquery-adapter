package com.socrata.bq.store

import com.socrata.bq.soql.BigQueryRepFactory

import scala.collection.JavaConversions._
import scala.annotation.tailrec

import com.socrata.soql.types._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.secondary.{CopyInfo => SecondaryCopyInfo, ColumnInfo => SecondaryColumnInfo, _}

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.ByteArrayContent
import com.google.api.services.bigquery.{Bigquery, BigqueryRequest}
import com.google.api.services.bigquery.model._
import com.rojoma.json.v3.ast._
import com.rojoma.simplearm.Managed
import com.rojoma.json.v3.util.JsonUtil
import com.typesafe.scalalogging.slf4j.Logging

class BBQResyncHandler(bigquery: Bigquery, bqProjectId: String, bqDatasetId: String) extends Logging {

  class BBQJob(job: Job) {
    val jobId = job.getJobReference.getJobId
    var state = job.getStatus.getState
    logger.debug(s"new bigquery job ${jobId} with state ${state}")

    def checkStatus: String = {
      val job = bigquery.jobs.get(bqProjectId, jobId).setFields("status").execute()
      val newState = job.getStatus.getState
      if (newState != state) {
        state = newState
        logger.debug(s"new state for job ${jobId}: ${state}")
        if (state == "DONE") {
          // Now that the job is done, we need to handle any errors that may have occurred.
          if (job.getStatus.getErrorResult != null) {
            logger.info(s"Final error message: ${job.getStatus.getErrorResult.getMessage}")
            job.getStatus.getErrors.foreach(e => logger.info(s"Error occurred: ${e.getMessage}"))
            throw new ResyncSecondaryException("Failed to load some data into bigquery")
          }
        }
        if (!List("PENDING", "RUNNING", "DONE").contains(state)) {
          logger.info(s"unexpected job state: ${state}")
        }
      }
      state
    }
  }

  private def parseDatasetId(datasetInternalName: String) = {
    datasetInternalName.split('.')(1).toInt
  }

  /**
   * Executes the request. If the request fails from a generic java.io.IOException, assumes a network error occurred,
   * and reissues the request after 500ms.
   * @return Some response, if the execution succeeds or eventually succeeds. None if it fails with an acceptalbe
   * response code.
   */
  private def executeAndAcceptResponseCodes[T](request: BigqueryRequest[T], acceptableResponseCodes: Int*) : Option[T] = {
    @tailrec def loop(): Option[T] = {
      try {
        return Some(request.execute())
      } catch {
        case e: GoogleJsonResponseException if acceptableResponseCodes contains e.getDetails.getCode =>
          return None
        case ioError: java.io.IOException =>
          logger.error(s"IOException occurred while executing a request to bigquery. Retrying in 5000ms.")
      }
      Thread.sleep(5000)
      loop()
    }
    loop()
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
          val content = new ByteArrayContent("application/octet-stream", batch.mkString("\n").toCharArray.map(_.toByte))
          val job = BigqueryUtils.makeLoadJob(ref)
          val insert = bigquery.jobs.insert(bqProjectId, job, content).setFields("jobReference,status")
          var jobResponse: Job = null
          try {
            jobResponse = insert.execute()
          } catch {
            // Assume any exception is ephemeral.
            case e: java.io.IOException =>
              throw new ResyncSecondaryException("An error occurred while sending a request to BigQuery")
          }
          new BBQJob(jobResponse)
        }

      // force jobs to execute
      var jobs = jobIterator.toList

      while (jobs.length > 0) {
        // does this thread sleep block other workers?
        Thread.sleep(1000)
        jobs = jobs.filter { job =>
          job.checkStatus != "DONE"
        }
      }
      logger.debug("done with all jobs")
    }
  }
}
