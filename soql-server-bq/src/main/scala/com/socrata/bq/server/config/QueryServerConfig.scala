package com.socrata.bq.server.config

import com.typesafe.config.Config
import com.socrata.http.server.livenesscheck.LivenessCheckConfig
import com.socrata.bq.config.StoreConfig
import com.socrata.thirdparty.curator.{CuratorConfig, DiscoveryConfig}
import com.socrata.thirdparty.metrics.MetricsOptions
import com.socrata.thirdparty.typesafeconfig.ConfigClass

class QueryServerConfig(val config: Config, val root: String) extends ConfigClass(config, root) {
  val log4j = getRawConfig("log4j")
  val store = new StoreConfig(config, path("store"))
  val port = getInt("port")
  val curator = new CuratorConfig(config, path("curator"))
  val discovery = new DiscoveryConfig(config, path("service-advertisement"))
  val livenessCheck = new LivenessCheckConfig(config, path("liveness-check"))
  val metrics = MetricsOptions(config.getConfig(path("metrics")))
  val instance = getString("instance")
  val threadpool = getRawConfig("threadpool")
  val bigqueryProjectId = getRawConfig("bigquery").getString("project-id")
  val bigqueryDatasetId = getRawConfig("bigquery").getString("dataset-id")
}
