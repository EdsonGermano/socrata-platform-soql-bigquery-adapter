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

  val tablespace = optionally(getString("tablespace"))

  val resyncBatchSize = optionally(getInt("resyncBatchSize")).getOrElse(1000)

}