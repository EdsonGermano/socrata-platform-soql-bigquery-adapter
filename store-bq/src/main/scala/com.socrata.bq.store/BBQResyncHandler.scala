package com.socrata.bq.store

import java.nio.charset.StandardCharsets.UTF_8
import scala.collection.JavaConversions._
import scala.annotation.tailrec

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.{AbstractInputStreamContent, ByteArrayContent}
import com.google.api.services.bigquery.{Bigquery, BigqueryRequest}
import com.google.api.services.bigquery.model._
import com.rojoma.json.v3.ast._
import com.rojoma.simplearm.Managed
import com.rojoma.json.v3.util.JsonUtil
import com.typesafe.scalalogging.slf4j.Logging

import com.socrata.soql.types._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.secondary.{CopyInfo => SecondaryCopyInfo, ColumnInfo => SecondaryColumnInfo, _}
import com.socrata.bq.soql.BigQueryRepFactory

import scala.collection.mutable.ArrayBuffer

class BBQResyncHandler(bigquery: Bigquery, bqProjectId: String, bqDatasetId: String) extends Logging {

  class BBQJob(job: Job) {
    val jobId = job.getJobReference.getJobId
    var state = job.getStatus.getState
    logger.debug(s"new bigquery job $jobId with state $state")

    @tailrec
    final def verify(retries: Int): Unit = {
      if (retries <= 0) {
        throw new RuntimeException("TODO: Figure out what to actually throw!")
      }
      
      try {
        val checkStatusJob = bigquery.jobs.get(bqProjectId, jobId).setFields("status").execute()
        val newState = checkStatusJob.getStatus.getState
        if (newState != state) {
          state = newState
          logger.debug(s"new state for job $jobId: $state")
          if (state == "DONE") {
            // Now that the job is done, we need to handle any errors that may have occurred.
            if (job.getStatus.getErrorResult != null) {
              logger.info(s"Final error message: ${checkStatusJob.getStatus.getErrorResult.getMessage}")
              checkStatusJob.getStatus.getErrors.foreach(e => logger.info(s"Error occurred: ${e.getMessage}"))
              //throw new ResyncSecondaryException("Failed to load some data into bigquery")
            }
          }
          if (!List("PENDING", "RUNNING", "DONE").contains(state)) {
            logger.info(s"unexpected job state: $state")
          }
        }

        if (state != "DONE") {
          logger.info(s"Job $jobId still $state. Checking status again... (retries remaining $retries)")
        }
      } catch {
        case e: java.io.IOException =>
          logger.info(s"Getting job status for $jobId failed. Retrying... (retries remaining $retries)")
      }

      if (state != "DONE") {
        Thread.sleep(1000)
        verify(retries - 1)
      }
    }
  }

  private def parseDatasetId(datasetInternalName: String) = {
    datasetInternalName.split('.')(1).toInt
  }

  /**
   * Executes the request. If the request fails from a generic java.io.IOException, assumes a network error occurred,
   * and reissues the request after 5000ms.
   * @return Some response, if the execution succeeds or eventually succeeds. None if it fails with an acceptalbe
   *         response code.
   */
  private def executeAndAcceptResponseCodes[T](request: BigqueryRequest[T], acceptableResponseCodes: Int*): Option[T] = {
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
  
  @tailrec
  private def loadContent(ref: TableReference,
                          content: AbstractInputStreamContent,
                          retries: Int): Job = {
    if (retries <= 0) {
      throw new RuntimeException("TODO: Figure out what to actually throw!")
    }
     
    try {
      val job = BigqueryUtils.makeLoadJob(ref)
      val insert = bigquery.jobs.insert(bqProjectId, job, content).setFields("jobReference,status")
      val jobResponse = insert.execute()

      if (jobResponse != null) {
        return jobResponse
      }
    } catch {
      // Assume that network-related exceptions are ephemeral.
      case e: java.io.IOException =>
        logger.info(s"Encountered a network exception: (${e.getClass}) ${e.getMessage}")
        logger.info("Retrying in 1000ms...")
        Thread.sleep(100)
      case e: Exception =>
        e.printStackTrace()
        throw e
    }

    loadContent(ref, content, retries - 1)
  }

  def handle(datasetInfo: DatasetInfo,
             copyInfo: SecondaryCopyInfo,
             schema: ColumnIdMap[SecondaryColumnInfo[SoQLType]],
             managedRowIterator: Managed[Iterator[ColumnIdMap[SoQLValue]]]): Unit = {
    logger.info(s"resyncing ${datasetInfo.internalName}")
    val datasetId = parseDatasetId(datasetInfo.internalName)
    // Make table reference and bigquery metadata
    val columnNames: ColumnIdMap[String] = BigqueryUtils.makeColumnNameMap(schema)
    val ref = BigqueryUtils.makeTableReference(bqProjectId, bqDatasetId, datasetInfo, copyInfo)
    val bqSchema = BigqueryUtils.makeTableSchema(schema, columnNames)
    val table = new Table()
      .setTableReference(ref)
      .setSchema(bqSchema)

    // Delete the table, just in case it already exists (from a failed resync, perhaps)
    val delete = bigquery.tables.delete(bqProjectId, bqDatasetId, ref.getTableId)
    executeAndAcceptResponseCodes(delete, 404)

    // Create the table
    val insert = bigquery.tables.insert(bqProjectId, bqDatasetId, table)
    executeAndAcceptResponseCodes(insert, 400)
    logger.info(s"Inserting into ${ref.getTableId}")

    managedRowIterator.foreach { rowIterator =>
      val batchSize = 2000 // TODO: Config?  Calculate Dynamically?

      val rows = rowIterator.grouped(batchSize).map { batch =>
        val buffer = new ArrayBuffer[Byte]

        batch.foreach { row =>
          val rowMap = row.foldLeft(Map[String, JValue]()) {
            case (map, (id, value)) =>
              columnNames.get(id) match {
                case None => map
                case Some(name) => map + ((name, BigQueryRepFactory(value.typ).jvalue(value)))
              }
          }

          // TODO: See if newlines are necessary
          val bytes = (JsonUtil.renderJson(rowMap) + "\n").getBytes(UTF_8)
          buffer.appendAll(bytes)
        }

        buffer.toArray
      }

      val jobIterator = rows.map { batch =>
        val content = new ByteArrayContent("application/octet-stream", batch)
        val jobResponse: Job = loadContent(ref, content, 20) // TODO: config

        new BBQJob(jobResponse)
      }

      // Check the status of all jobs for completion
      jobIterator.foreach(_.verify(100)) // TODO: Use config!
      logger.debug("done with all jobs")
    }
  }
}
