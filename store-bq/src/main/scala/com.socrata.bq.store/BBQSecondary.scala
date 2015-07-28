package com.socrata.bq.store

import com.socrata.bq.soql.BigQueryRepFactory

import collection.JavaConversions._
import java.sql.{Connection, DriverManager, ResultSet}

import com.rojoma.json.v3.ast._
import com.rojoma.simplearm.util._
import com.rojoma.simplearm.Managed
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.soql.types._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.common.DataSourceConfig
import com.socrata.datacoordinator.common.DataSourceFromConfig.DSInfo
import com.socrata.datacoordinator.secondary.{CopyInfo => SecondaryCopyInfo, ColumnInfo => SecondaryColumnInfo, _}
import com.socrata.datacoordinator.secondary.Secondary.Cookie
import com.socrata.datacoordinator.id.{DatasetId, CopyId, ColumnId, UserColumnId}
import com.socrata.datacoordinator.truth.universe.sql.PostgresCopyIn
import com.socrata.thirdparty.typesafeconfig.C3P0Propertizer
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.Logging
import org.postgresql.ds.PGSimpleDataSource

import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.ByteArrayContent
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.{Bigquery, BigqueryScopes}
import com.google.api.services.bigquery.model._

class BBQSecondary(config: Config) extends Secondary[SoQLType, SoQLValue] with Logging {

  private val PROJECT_ID = config.getConfig("bigquery").getString("project-id")
  private val BQ_DATASET_ID = config.getConfig("bigquery").getString("dataset-id")
  private val TRANSPORT = new NetHttpTransport()
  private val JSON_FACTORY = new JacksonFactory()

  val bigquery = {
    var credential: GoogleCredential = GoogleCredential.getApplicationDefault()
    if (credential.createScopedRequired) {
      credential = credential.createScoped(BigqueryScopes.all)
    }
    new Bigquery.Builder(TRANSPORT, JSON_FACTORY, credential).setApplicationName("BBQ Secondary").build()
  }

  // called on graceful shutdown
  override def shutdown(): Unit = {
    logger.info("shutdown called")
  }

  override def dropDataset(datasetInternalName: String, cookie: Cookie): Unit = {
    logger.info("dropDataset called")
  }

  override def snapshots(datasetInternalName: String, cookie: Cookie): Set[Long] = ???

  override def dropCopy(datasetInternalName: String, copyNumber: Long, cookie: Cookie): Cookie = {
    logger.info(s"dropCopy called on ${datasetInternalName}/${copyNumber}")
    cookie
  }

  override def currentCopyNumber(datasetInternalName: String, cookie: Cookie): Long = {
    val datasetId = parseDatasetId(datasetInternalName)
    bigqueryUtils.getCopyNumber(datasetId)
  }

  override def wantsWorkingCopies: Boolean = {
    println("wantsWorkingCopies called")
    false
  }

  override def currentVersion(datasetInternalName: String, cookie: Cookie): Long = {
    val datasetId = parseDatasetId(datasetInternalName)
    bigqueryUtils.getDataVersion(datasetId)
  }

  override def resync(datasetInfo: DatasetInfo,
                      copyInfo: SecondaryCopyInfo,
                      schema: ColumnIdMap[SecondaryColumnInfo[SoQLType]],
                      cookie: Secondary.Cookie,
                      rows: Managed[Iterator[ColumnIdMap[SoQLValue]]],
                      rollups: Seq[RollupInfo]): Secondary.Cookie = {
    logger.info(s"resyncing ${datasetInfo.internalName}")
    val datasetId = parseDatasetId(datasetInfo.internalName)
    // make table reference and bigquery metadata
    val columnNames: ColumnIdMap[String] = bigqueryUtils.makeColumnNameMap(schema)
    val ref = bigqueryUtils.makeTableReference(BQ_DATASET_ID, datasetInfo, copyInfo)
    val bqSchema = bigqueryUtils.makeTableSchema(schema, columnNames)
    val table = new Table()
            .setTableReference(ref)
            .setSchema(bqSchema)

    try {
      bigquery.tables.insert(PROJECT_ID, BQ_DATASET_ID, table).execute()
      logger.info(s"Inserting into ${ref.getTableId}")
    } catch {
      case e: GoogleJsonResponseException => {
        if (e.getDetails.getCode == 409) {
          // the table already exists
          // what should be done here?
        } else {
          throw e
        }
      }
    }
    for { iter <- rows } {
      val requests =
        for {
          row: ColumnIdMap[SoQLValue] <- iter
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
        val insert = bigquery.jobs.insert(PROJECT_ID, bigqueryUtils.makeLoadJob(ref), content)
        insert.execute()
      }
    }
    bigqueryUtils.setCopyInfoEntry(datasetId, copyInfo)
    cookie
  }

  override def version(datasetInfo: DatasetInfo,
              dataVersion: Long,
              cookie: Cookie,
              events: Iterator[Event[SoQLType, SoQLValue]]): Cookie = {
    val internalName = datasetInfo.internalName
    logger.info(s"version called for dataset ${internalName}@${dataVersion}")

    events.foreach {
      case WorkingCopyPublished => {
        logger.info(">> got publish event; throwing resync")
        throw new ResyncSecondaryException("resync from WorkingCopyPublished")
      }
      case _ => ()
    }

    cookie
  }

  private def parseDatasetId(datasetInternalName: String) = {
    datasetInternalName.split('.')(1).toInt
  }

  private def dataSourceFromConfig(config: DataSourceConfig): DSInfo = {
    val dataSource = new PGSimpleDataSource
    dataSource.setServerName(config.host)
    dataSource.setPortNumber(config.port)
    dataSource.setDatabaseName(config.database)
    dataSource.setUser(config.username)
    dataSource.setPassword(config.password)
    dataSource.setApplicationName(config.applicationName)
    new DSInfo(dataSource, PostgresCopyIn)
  }

  private val dsInfo = dataSourceFromConfig(new DataSourceConfig(config, "database"))

  private val bigqueryUtils = new BigqueryUtils(dsInfo, PROJECT_ID)
}
