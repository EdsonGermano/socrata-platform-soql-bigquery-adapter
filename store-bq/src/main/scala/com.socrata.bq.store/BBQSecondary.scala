package com.socrata.bq.store

import com.google.api.client.googleapis.json.GoogleJsonResponseException

import collection.JavaConversions._
import com.rojoma.simplearm.Managed
import com.socrata.soql.types._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.common.DataSourceConfig
import com.socrata.datacoordinator.common.DataSourceFromConfig.DSInfo
import com.socrata.datacoordinator.secondary.{CopyInfo => SecondaryCopyInfo, ColumnInfo => SecondaryColumnInfo, _}
import com.socrata.bq.SecondaryBase
import com.socrata.datacoordinator.secondary.Secondary.Cookie
import com.socrata.datacoordinator.truth.universe.sql.PostgresCopyIn
import com.socrata.datacoordinator.truth.metadata.{CopyInfo => TruthCopyInfo, LifecycleStage}
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.Logging
import org.postgresql.ds.PGSimpleDataSource
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.{Bigquery, BigqueryScopes}

class BBQSecondary(config: Config) extends Secondary[SoQLType, SoQLValue] with SecondaryBase with Logging {

  val storeConfig = new StoreConfig(config, "")

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

  override val postgresUniverseCommon = new PostgresUniverseCommon(TablespaceFunction(storeConfig.tablespace), dsInfo.copyIn)

  private val bigqueryUtils = new BigqueryUtils(dsInfo, storeConfig.projectId)

  private val resyncHandler = new BBQResyncHandler(storeConfig.resyncConfig, bigquery, storeConfig.projectId, storeConfig.datasetId)

  // called on graceful shutdown
  override def shutdown(): Unit = {
    logger.info("shutdown called")
  }

  override def dropDataset(datasetInternalName: String, cookie: Cookie): Unit = {
    logger.info(s"dropDataset called on $datasetInternalName")
    // The following block (except _dropCopy) was extracted from PGSecondary
    withPgu(dsInfo, None) { pgu =>
      truthCopyInfo(pgu, datasetInternalName) match {
        case Some(copyInfo) =>
          pgu.datasetMapWriter.delete(copyInfo.datasetInfo)
          pgu.commit()
          _dropCopy(datasetInternalName, copyInfo.copyNumber)
        case None =>
          // TODO: Clean possible orphaned dataset internal name map
          logger.warn("drop dataset called but cannot find a copy {}", datasetInternalName)
      }
    }
  }

  override def snapshots(datasetInternalName: String, cookie: Cookie): Set[Long] = ???

  override def dropCopy(datasetInternalName: String, copyNumber: Long, cookie: Cookie): Cookie = {
    // PGSecondary doesn't implement this, so we won't either
    logger.warn(s"TODO: dropCopy '${datasetInternalName}' (cookie: ${cookie}})")
    cookie
  }

  private def _dropCopy(datasetInternalName, copyNumber) {
    try {
      bigquery.tables().delete(storeConfig.projectId, storeConfig.datasetId, bigqueryUtils.makeTableName(datasetInternalName, copyNumber)).execute()
    } catch {
      case e: GoogleJsonResponseException if e.getDetails.getCode == 404 =>
      case _: Throwable => logger.info(s"Encountered an error while deleting $datasetInternalName/$copyNumber")
    }
  }

  override def currentCopyNumber(datasetInternalName: String, cookie: Cookie): Long = {
    withPgu(dsInfo, None) { pgu =>
      truthCopyInfo(pgu, datasetInternalName) match {
        case Some(copyInfo) => copyInfo.copyNumber
        case None => 
          logger.warn("current copy number called but cannot find a copy {}", datasetInternalName)
          0
      }
    }
  }

  override def wantsWorkingCopies: Boolean = {
    println("wantsWorkingCopies called")
    false
  }

  override def currentVersion(datasetInternalName: String, cookie: Cookie): Long = {
    withPgu(dsInfo, None) { pgu =>
      truthCopyInfo(pgu, datasetInternalName) match {
        case Some(copyInfo) => copyInfo.dataVersion
      case None =>
        logger.warn("current version called but cannot find a copy {}", datasetInternalName)
        0
      }
    }
  }

  override def resync(datasetInfo: DatasetInfo,
                      copyInfo: SecondaryCopyInfo,
                      schema: ColumnIdMap[SecondaryColumnInfo[SoQLType]],
                      cookie: Secondary.Cookie,
                      rows: Managed[Iterator[ColumnIdMap[SoQLValue]]],
                      rollups: Seq[RollupInfo]): Secondary.Cookie = {
    withPgu(dsInfo, Some(datasetInfo)) { pgu =>
      pgu.datasetMapWriter.deleteCopy(copyInfo)
      pgu.datasetMapWriter.unsafeCreateCopy(datasetInfo, copyInfo.systemId, copyInfo.copyNumber, LifecycleStage.Published, copyInfo.dataVersion)
      pgu.commit()
    }
    resyncHandler.handle(datasetInfo, copyInfo, schema, rows)
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

  private def truthCopyInfo(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], datasetInternalName: String): Option[TruthCopyInfo] = {
    for {
      datasetId <- pgu.secondaryDatasetMapReader.datasetIdForInternalName(datasetInternalName)
      truthDatasetInfo <- pgu.datasetMapReader.datasetInfo(datasetId)
    } yield {
      pgu.datasetMapReader.latest(truthDatasetInfo)
    }
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
