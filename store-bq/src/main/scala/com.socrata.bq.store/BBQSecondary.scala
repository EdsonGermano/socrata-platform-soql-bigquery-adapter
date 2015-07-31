package com.socrata.bq.store

import com.socrata.bq.soql.BigQueryRepFactory

import collection.JavaConversions._
import java.sql.{Connection, DriverManager, ResultSet}

import com.rojoma.json.v3.ast._
import com.rojoma.simplearm.util._
import com.rojoma.simplearm.Managed
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
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.{Bigquery, BigqueryScopes}
import com.google.api.services.bigquery.model._

class BBQSecondary(config: Config) extends Secondary[SoQLType, SoQLValue] with Logging {

  private val BQ_PROJECT_ID = config.getConfig("bigquery").getString("project-id")
  private val BQ_DATASET_ID = config.getConfig("bigquery").getString("dataset-id")
  private val TRANSPORT = new NetHttpTransport()
  private val JSON_FACTORY = new JacksonFactory()

  private val bigquery = {
    var credential: GoogleCredential = GoogleCredential.getApplicationDefault()
    if (credential.createScopedRequired) {
      credential = credential.createScoped(BigqueryScopes.all)
    }
    new Bigquery.Builder(TRANSPORT, JSON_FACTORY, credential).setApplicationName("BBQ Secondary").build()
  }

  private val dsInfo = dataSourceFromConfig(new DataSourceConfig(config, "database"))

  private val bigqueryUtils = new BigqueryUtils(dsInfo, BQ_PROJECT_ID)

  private val resyncHandler = new BBQResyncHandler(BQ_PROJECT_ID, BQ_DATASET_ID)

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
    bigqueryUtils.getCopyNumber(datasetId).getOrElse(0)
  }

  override def wantsWorkingCopies: Boolean = {
    println("wantsWorkingCopies called")
    false
  }

  override def currentVersion(datasetInternalName: String, cookie: Cookie): Long = {
    val datasetId = parseDatasetId(datasetInternalName)
    bigqueryUtils.getDataVersion(datasetId).getOrElse(0)
  }

  override def resync(datasetInfo: DatasetInfo,
                      copyInfo: SecondaryCopyInfo,
                      schema: ColumnIdMap[SecondaryColumnInfo[SoQLType]],
                      cookie: Secondary.Cookie,
                      rows: Managed[Iterator[ColumnIdMap[SoQLValue]]],
                      rollups: Seq[RollupInfo]): Secondary.Cookie = {
    val datasetId = parseDatasetId(datasetInfo.internalName)
    resyncHandler.handle(bigquery, datasetInfo, copyInfo, schema, rows)
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
}
