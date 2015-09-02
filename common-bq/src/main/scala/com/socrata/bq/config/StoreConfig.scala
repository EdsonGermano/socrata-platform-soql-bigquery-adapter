package com.socrata.bq.config

import com.typesafe.config.{ConfigUtil, Config}
import com.socrata.thirdparty.typesafeconfig.ConfigClass
import com.socrata.datacoordinator.common.DataSourceConfig
import scala.collection.JavaConverters._

class StoreConfig(config: Config, root: String) extends ConfigClass(config, root) {

  // handle blank root
  override protected def path(key: String*) = {
    val fullKey = if (root.isEmpty) key else ConfigUtil.splitPath(root).asScala ++ key
    ConfigUtil.joinPath(fullKey: _*)
  }

  val database = new DataSourceConfig(config, path("database"))

  private val bigqueryConfig = config.getConfig("bigquery")
  val projectId = bigqueryConfig.getString("project-id")
  val datasetId = bigqueryConfig.getString("dataset-id")

  val tablespace = optionally(getString("tablespace"))

  val resyncConfig = config.getConfig("resync-handler")

}