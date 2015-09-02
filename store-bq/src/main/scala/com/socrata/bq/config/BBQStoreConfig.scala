package com.socrata.bq.config

import com.socrata.datacoordinator.common.DataSourceConfig
import com.socrata.thirdparty.typesafeconfig.ConfigClass
import com.typesafe.config.{Config, ConfigUtil}
import scala.collection.JavaConverters._

class BBQStoreConfig(config: Config, root: String) extends ConfigClass(config, root) {

  // handle blank root
  override protected def path(key: String*) = {
    val fullKey = if (root.isEmpty) key else ConfigUtil.splitPath(root).asScala ++ key
    ConfigUtil.joinPath(fullKey: _*)
  }

  // Database
  val database = new DataSourceConfig(config, path("database"))

  // Bigquery
  private val bigqueryConfig = config.getConfig("bigquery")
  val projectId = bigqueryConfig.getString("project-id")
  val datasetId = bigqueryConfig.getString("dataset-id")

  // Resync handler
  val resyncConfig = config.getConfig("resync-handler")
}
