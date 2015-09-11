package com.socrata.bq.store

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.{Bigquery, BigqueryScopes}

import collection.JavaConversions._
import com.rojoma.simplearm.Managed
import com.socrata.soql.types._
import com.socrata.bq.config.{BBQStoreConfig}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.common.DataSourceConfig
import com.socrata.datacoordinator.common.DataSourceFromConfig.DSInfo
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.secondary.Secondary.Cookie
import com.socrata.datacoordinator.truth.universe.sql.PostgresCopyIn
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.Logging
import org.postgresql.ds.PGSimpleDataSource

class BBQSecondary(config: Config) extends Secondary[SoQLType, SoQLValue] with Logging {

  private val copyTable = "bbq_copy_info_2"

  logger.info(s"config: ${config.toString}")

  val storeConfig = new BBQStoreConfig(config, "")

  private val TRANSPORT = new NetHttpTransport()
  private val JSON_FACTORY = new JacksonFactory()
  logger.debug(s"Using project ${storeConfig.projectId} with dataset ${storeConfig.datasetId}")

  private val bigquery = {
    var credential: GoogleCredential = GoogleCredential.getApplicationDefault
    if (credential.createScopedRequired) {
      credential = credential.createScoped(BigqueryScopes.all)
    }
    new Bigquery.Builder(TRANSPORT, JSON_FACTORY, credential).setApplicationName("BBQ Secondary").build()
  }

  private val dsInfo = dataSourceFromConfig(storeConfig.database)

  private val bigqueryUtils = new BigqueryUtils(dsInfo, storeConfig.projectId)

  private val resyncHandler = new BBQResyncHandler(storeConfig.resyncConfig, bigquery, storeConfig.projectId, storeConfig.datasetId)

  // called on graceful shutdown
  override def shutdown(): Unit = {
    logger.info("shutdown called")
  }

  override def dropDataset(datasetInternalName: String, cookie: Cookie): Unit = {
    logger.info(s"dropDataset called on $datasetInternalName")
    val currentCopyNum = currentCopyNumber(datasetInternalName, cookie)
    logger.info(s"Calling dropCopy on copies 1 through $currentCopyNum")
    for (i <- 1 to currentCopyNum.toInt) {
      dropCopy(datasetInternalName, i.toLong, cookie)
    }
  }

  override def snapshots(datasetInternalName: String, cookie: Cookie): Set[Long] = ???

  override def dropCopy(datasetInternalName: String, copyNumber: Long, cookie: Cookie): Cookie = {
    logger.info(s"dropCopy called on $datasetInternalName/$copyNumber")
    try {
      bigquery.tables().delete(storeConfig.projectId, storeConfig.datasetId, bigqueryUtils.makeTableName(datasetInternalName, copyNumber)).execute()
    } catch {
      case e: GoogleJsonResponseException if e.getDetails.getCode == 404 =>
      case _: Throwable => logger.info(s"Encountered an error while deleting $datasetInternalName/$copyNumber")
    }
    cookie
  }

  override def currentCopyNumber(datasetInternalName: String, cookie: Cookie): Long = {
    logger.info(s"currentCopyNumber called for $datasetInternalName")
    bigqueryUtils.getMetadataEntry(datasetInternalName) match {
      case Some(bbqDatasetInfo) => bbqDatasetInfo.copyNumber
      case None =>
        logger.warn(s"Could not find current copy number for $datasetInternalName")
        0
    }
  }

  override def wantsWorkingCopies: Boolean = {
    logger.info("wantsWorkingCopies called")
    false
  }

  override def currentVersion(datasetInternalName: String, cookie: Cookie): Long = {
    logger.info(s"currentVersion called for $datasetInternalName")
    bigqueryUtils.getMetadataEntry(datasetInternalName) match {
      case Some(bbqDatasetInfo) => bbqDatasetInfo.dataVersion
      case None =>
        logger.warn(s"Could not find a version for $datasetInternalName")
        0
    }
  }

  override def resync(datasetInfo: DatasetInfo,
                      copyInfo: CopyInfo,
                      schema: ColumnIdMap[ColumnInfo[SoQLType]],
                      cookie: Secondary.Cookie,
                      rows: Managed[Iterator[ColumnIdMap[SoQLValue]]],
                      rollups: Seq[RollupInfo]): Secondary.Cookie = {
    logger.info(s"resync called for dataset ${datasetInfo.internalName}")
    resyncHandler.handle(datasetInfo, copyInfo, schema, rows)
    bigqueryUtils.setMetadataEntry(datasetInfo, copyInfo)
    bigqueryUtils.setSchema(datasetInfo, schema)
    cookie
  }

  override def version(datasetInfo: DatasetInfo,
                       dataVersion: Long,
                       cookie: Cookie,
                       events: Iterator[Event[SoQLType, SoQLValue]]): Cookie = {
    val internalName = datasetInfo.internalName
    logger.info(s"version called for dataset $internalName@$dataVersion")

    events.foreach {
      case WorkingCopyPublished => {
        logger.info(">> got publish event; throwing resync")
        throw new ResyncSecondaryException("resync from WorkingCopyPublished")
      }
      case _ => ()
    }

    cookie
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
