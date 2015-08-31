package com.socrata.bq.store

import java.nio.charset.StandardCharsets.UTF_8
import scala.collection.JavaConversions._
import scala.annotation.tailrec

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.{AbstractInputStreamContent, ByteArrayContent}
import com.google.api.services.bigquery.{Bigquery, BigqueryRequest}
import com.google.api.services.bigquery.model._
import com.rojoma.json.v3.ast._
import com.typesafe.config.Config
import com.rojoma.simplearm.Managed
import com.rojoma.json.v3.util.JsonUtil
import com.typesafe.scalalogging.slf4j.Logging

import com.socrata.soql.types._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.secondary.{CopyInfo => SecondaryCopyInfo, ColumnInfo => SecondaryColumnInfo, _}
import com.socrata.bq.soql.BigQueryRepFactory

import scala.collection.mutable.ArrayBuffer

class BBQResyncHandler(config: Config, val bigquery: Bigquery, val bqProjectId: String, val bqDatasetId: String) extends Logging {
  val BATCH_SIZE: Int = config.getInt("batch-size")
  val INSERT_TIMEOUT: Int = config.getInt("insert-timeout")
  val INSERT_RETRIES: Int = config.getInt("insert-retries")
  val JOB_STATUS_TIMEOUT: Int = config.getInt("job-status-timeout")
  val JOB_STATUS_RETRIES: Int = config.getInt("job-status-retries")

  class BBQJob(job: Job) {
    val jobId = job.getJobReference.getJobId
    var state = job.getStatus.getState
    logger.debug(s"New bigquery job $jobId with state $state")

    @tailrec
    final def verify(retries: Int): Unit = {
      if (retries <= 0) {
        throw new RuntimeException(s"Ran out of retries while verifying $jobId completed")
      }

      try {
        val verifyJob = bigquery.jobs.get(bqProjectId, jobId).setFields("status,statistics").execute()
        val status = verifyJob.getStatus
        val newState = status.getState
        val statistics = verifyJob.getStatistics
        if (newState != state) {
          state = newState
          logger.info(s"$jobId is now $state")
          if (state == "DONE") {
            // Now that the job is done, we need to handle any errors that may have occurred.
            if (status.getErrorResult != null) {
              logger.info(s"Final error message: ${status.getErrorResult.getMessage}")
              status.getErrors.foreach(e => logger.info(s"Error occurred: ${e.getMessage}"))
              throw new RuntimeException("TODO: Figure out what to actually throw!")
            }
            // Log job statistics
            if (statistics != null && statistics.getLoad != null) {
              logger.info(s"${statistics.getLoad.getOutputBytes} bytes processed")
              logger.info(s"${statistics.getLoad.getOutputRows} rows processed")
            }
          }
          if (!List("PENDING", "RUNNING", "DONE").contains(state)) {
            logger.info(s"unexpected job state: $state")
          }
        }

        if (state != "DONE") {
          logger.info(s"$jobId still $state. Checking status again... (retries remaining $retries)")
        }
      } catch {
        case e: java.io.IOException =>
          logger.info(s"Getting job status for $jobId failed. Retrying... (retries remaining $retries)")
      }

      if (state != "DONE") {
        Thread.sleep(JOB_STATUS_TIMEOUT)
        verify(retries - 1)
      }
    }
  }


  // BoundedBuffer is append-only
  class BoundedBuffer(size: Int) {
    private val array = new Array[Byte](size)
    private var length = 0
    // Appends a string and a newline if there is enough space. Returns true on success, false otherwise
    def append(row: String): Boolean = {
      if (length + row.length + 1 > size) {
        return false
      } else {
        for (char <- row) {
          array(length) = char.toByte
          length += 1
        }
        array(length) = '\n'.toByte
        length += 1
        return true
      }
    }

    private def toArray(): Array[Byte] = {
      array.slice(0, length)
    }

    def toByteArrayContent(): ByteArrayContent = {
      new ByteArrayContent("application/octet-stream", toArray())
    }

    def hasContent(): Boolean = {
      length != 0
    }

    def clear(): Unit = {
      length = 0
    }
  }
  private def parseDatasetId(datasetInternalName: String) = {
    datasetInternalName.split('.')(1).toInt
  }

  /**
   * Executes the request. If the request fails from a generic java.io.IOException, assumes a network error occurred,
   * and reissues the request after 5000ms.
   * @return Some response, if the execution succeeds or eventually succeeds. None if it fails with an acceptable
   *         response code.
   */
    private def executeAndAcceptResponseCodes[T](request: BigqueryRequest[T], acceptableResponseCode: Int): Option[T] = {

      @tailrec
      def loop(retries: Int): Option[T] = {
        if (retries <= 0) {
          throw new RuntimeException("Ran out of retries")
        }

        try {
          return Some(request.execute())
        } catch {
          case e: GoogleJsonResponseException if acceptableResponseCode == e.getDetails.getCode =>
            return None
          case ioError: java.io.IOException =>
            logger.error(s"Encountered a network exception: (${ioError.getClass}) ${ioError.getMessage}")
        }

        Thread.sleep(INSERT_TIMEOUT)
        loop(retries - 1)
      }

      loop(INSERT_RETRIES)
  }

  @tailrec
  private def loadContent(ref: TableReference,
                          content: AbstractInputStreamContent,
                          retries: Int): Job = {
    if (retries <= 0) {
      throw new RuntimeException("Ran out of retries while sending insert job")
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
        logger.info(s"Encountered a network exception: (${e.getClass}) ${e.getMessage} (Retries remaining: ${retries})")
        Thread.sleep(INSERT_TIMEOUT)
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
    logger.info(s"Resyncing ${datasetInfo.internalName}")
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
      val buffer = new BoundedBuffer(BATCH_SIZE)
      var insertJobs = rowIterator.foldLeft(Seq[Job]()) { (jobs: Seq[Job], row: ColumnIdMap[SoQLValue]) =>
          val rowMap = row.foldLeft(Map[String, JValue]()) {
            case (map, (id, value)) =>
              columnNames.get(id) match {
                case None => map
                case Some(name) => map + ((name, BigQueryRepFactory(value.typ).jvalue(value)))
              }
          }
          val jsonifiedRow = JsonUtil.renderJson(rowMap)
          buffer.append(jsonifiedRow) match {
            case false =>
              val jobResponse = loadContent(ref, buffer.toByteArrayContent(), INSERT_RETRIES)
              logger.info("Buffer is full, sending insert job")
              buffer.clear()
              buffer.append(jsonifiedRow)
              jobs :+ jobResponse
            case true => jobs
          }
        }.toList // Force execution of jobs

      // Final check to make sure that all rows get sent
      logger.info("Checking buffer and send off final insert job")
      insertJobs = insertJobs :+ loadContent(ref, buffer.toByteArrayContent(), INSERT_RETRIES)
      buffer.clear()

      // Check the status of all jobs for completion
      insertJobs.foreach(new BBQJob(_).verify(JOB_STATUS_RETRIES))
      logger.debug("Done with all jobs")
    }
  }
}
