package com.socrata.bq.server

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.sql.Connection
import java.util.concurrent.{ExecutorService, Executors}
import com.socrata.bq.soql.bqreps.MultiPolygonRep.BoundingBoxRep
import com.socrata.bq.store.BBQCommon

import scala.language.existentials
import com.rojoma.json.v3.ast.JString
import com.rojoma.json.v3.util.JsonUtil
import com.rojoma.simplearm.Managed
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.Row
import com.socrata.datacoordinator.common.{DataSourceConfig, DataSourceFromConfig}
import com.socrata.datacoordinator.common.DataSourceFromConfig.DSInfo
import com.socrata.datacoordinator.common.soql.SoQLTypeContext
import com.socrata.datacoordinator.id.{ColumnId, UserColumnId}
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.util.CloseableIterator
import com.socrata.http.common.AuxiliaryData
import com.socrata.http.common.livenesscheck.LivenessCheckInfo
import com.socrata.http.server._
import com.socrata.http.server.curator.CuratorBroker
import com.socrata.http.server.implicits._
import com.socrata.http.server.livenesscheck.LivenessCheckResponder
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.{SimpleResource, SimpleRouteContext}
import com.socrata.http.server.util.{EntityTag, NoPrecondition, Precondition, StrongEntityTag}
import com.socrata.http.server.util.Precondition._
import com.socrata.http.server.util.handlers.{NewLoggingHandler, ThreadRenamingHandler}
import com.socrata.http.server.util.RequestId.ReqIdHeader
import com.socrata.bq.Version
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.bq.Schema._
import com.socrata.bq.query._
import com.socrata.bq.server.config.{DynamicPortMap, QueryServerConfig}
import com.socrata.bq.soql._
import com.socrata.bq.soql.SqlizerContext.SqlizerContext
import com.socrata.bq.store._
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.analyzer.SoQLAnalyzerHelper
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.typed.{FunctionCall, CoreExpr}
import com.socrata.soql.types._
import com.socrata.soql.types.obfuscation.CryptProvider
import com.socrata.thirdparty.curator.{CuratorFromConfig, DiscoveryFromConfig}
import com.socrata.thirdparty.typesafeconfig.Propertizer
import com.socrata.thirdparty.metrics.{SocrataHttpSupport, MetricsReporter}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.slf4j.Logging
import javax.servlet.http.HttpServletResponse
import org.apache.curator.x.discovery.ServiceInstanceBuilder
import org.apache.log4j.PropertyConfigurator
import org.joda.time.DateTime

class QueryServer(val config: QueryServerConfig, val bqUtils: BBQCommon, val dsInfo: DSInfo, val caseSensitivity: CaseSensitivity) extends Logging {
  import QueryServer._

  private val JsonContentType = "application/json; charset=utf-8"

  private val routerSet = locally {
    import SimpleRouteContext._
    Routes(
      Route("/schema", SchemaResource),
      Route("/query", QueryResource),
      Route("/version", VersionResource)
    )
  }

  private def route(req: HttpRequest): HttpResponse = {
    routerSet(req.requestPath) match {
      case Some(s) =>
        s(req)
      case None =>
        logger.info(s"Requested path not found: ${req.requestPath}")
        NotFound
    }
  }

  object VersionResource extends SimpleResource {
    val response = OK ~> Json(Version("soql-server-bq"))

    override val get = { _: HttpRequest => response }
  }

  object SchemaResource extends SimpleResource {
    override val get = schema _
  }

  def schema(req: HttpRequest): HttpResponse = {
    val servReq = req.servletRequest
    val ds = servReq.getParameter("ds")
    val copy = Option(servReq.getParameter("copy"))
    getSchema(ds) match {
      case Some(schemaResult) =>
        val (copyNum: Long, versionNum: Long, lastModified: DateTime) = bqUtils.getMetadataEntry(ds) match {
          case Some(cinfo) => (cinfo.copyNumber, cinfo.dataVersion, cinfo.lastModified)
          case None => (0L, 0L)
        }
        logger.debug(s"Found schema for dataset $ds")
        OK ~>
          copyInfoHeader(copyNum, versionNum, lastModified) ~>
          Write(JsonContentType)(JsonUtil.writeJson(_, schemaResult, buffer = true))
      case None =>
        logger.debug(s"Cannot find schema for dataset $ds")
        NotFound
    }
  }

  object QueryResource extends SimpleResource {
    override val get = query _
    override val post = query _
  }

  def etagFromCopy(datasetInternalName: String, copyNum: Long, versionNum: Long): EntityTag = {
    // ETag is a hash based on datasetInternalName_copyNumber_version
    val etagContents = s"${datasetInternalName}_${copyNum}_${versionNum}"
    StrongEntityTag(etagContents.getBytes(StandardCharsets.UTF_8))
  }

  def query(req: HttpRequest): HttpServletResponse => Unit =  {
    val servReq = req.servletRequest
    val datasetId = servReq.getParameter("dataset")
    val analysisParam = servReq.getParameter("query")
    val analysisStream = new ByteArrayInputStream(analysisParam.getBytes(StandardCharsets.ISO_8859_1))
    val schemaHash = servReq.getParameter("schemaHash")
    val analysis: SoQLAnalysis[UserColumnId, SoQLType] = SoQLAnalyzerHelper.deserializer(analysisStream)
    val reqRowCount = Option(servReq.getParameter("rowCount")).map(_ == "approximate").getOrElse(false)
    val copy = Option(servReq.getParameter("copy"))

    logger.debug("Performing query on dataset " + datasetId)
    streamQueryResults(analysis, datasetId, reqRowCount, copy, req.precondition, req.dateTimeHeader("If-Modified-Since"))
  }

  /**
   * Stream the query results; we need to have the entire HttpServletResponse => Unit
   * passed back to SocrataHttp so the transaction can be maintained through the duration of the
   * streaming.
   */
  def streamQueryResults(
    analysis: SoQLAnalysis[UserColumnId, SoQLType],
    datasetName:String,
    reqRowCount: Boolean,
    copy: Option[String],
    precondition: Precondition,
    ifModifiedSince: Option[DateTime]
  ) (resp:HttpServletResponse) = {
    val datasetId = new DatasetId(bqUtils.parseDatasetId(datasetName))
    bqUtils.getMetadataEntry(datasetName) match {
      case Some(cinfo) =>
        logger.debug(s"Found metadata for dataset $datasetName")
        def notModified(etags: Seq[EntityTag]) = responses.NotModified ~> ETags(etags)
        execQuery(datasetName, cinfo, analysis, reqRowCount, copy, precondition, ifModifiedSince) match {
          case Success(qrySchema, copyNumber, dataVersion, results, etag, lastModified) =>
            // Very weird separation of concerns between execQuery and streaming. Most likely we will
            // want yet-another-refactoring where much of execQuery is lifted out into this function.
            // This will significantly change the tests; however.
            logger.debug("Success, writing results with CJSONWriter")
            ETag(etag)(resp)
            copyInfoHeader(copyNumber, dataVersion, lastModified)(resp)
            for (r <- results) yield {
              CJSONWriter.writeCJson(Some(cinfo.obfuscationKey), qrySchema, r, reqRowCount, dataVersion, lastModified)(resp)
            }
          case NotModified(etags) =>
            notModified(etags)(resp)
          case PreconditionFailed =>
            responses.PreconditionFailed(resp)
        }
      case None =>
        logger.info(s"No metadata entry for dataset $datasetName")
        NotFound(resp)
    }
  }

  def execQuery(
    datasetInternalName: String,
    cinfo: BBQDatasetInfo,
    analysis: SoQLAnalysis[UserColumnId, SoQLType],
    rowCount: Boolean,
    reqCopy: Option[String],
    precondition: Precondition,
    ifModifiedSince: Option[DateTime]
  ): QueryResult = {

    val copyNumber = cinfo.copyNumber
    val versionNumber = cinfo.dataVersion
    val lastModified = cinfo.lastModified
    val etag = etagFromCopy(datasetInternalName, copyNumber, versionNumber)

    // Conditional GET handling
    precondition.check(Some(etag), sideEffectFree = true) match {
      case Passed =>
        ifModifiedSince match {
          case Some(ims) if !lastModified.minusMillis(lastModified.getMillisOfSecond).isAfter(ims)
            && precondition == NoPrecondition =>
            NotModified(Seq(etag))
          case Some(_) | None =>
            val (qrySchema, results) = runQuery(analysis, cinfo, datasetInternalName, rowCount)
            Success(qrySchema, copyNumber, versionNumber, results, etag, lastModified)
        }
      case FailedBecauseMatch(etags) =>
        NotModified(etags)
      case FailedBecauseNoMatch =>
        PreconditionFailed
    }
  }

  def runQuery(analysis: SoQLAnalysis[UserColumnId, SoQLType], cinfo: BBQDatasetInfo, datasetInternalName: String, rowCount: Boolean) = {
    val cryptProvider = new CryptProvider(cinfo.obfuscationKey)
    val sqlCtx = Map[SqlizerContext, Any](
      SqlizerContext.IdRep -> new SoQLID.StringRep(cryptProvider),
      SqlizerContext.VerRep -> new SoQLVersion.StringRep(cryptProvider),
      SqlizerContext.CaseSensitivity -> caseSensitivity
    )

    // A function that escapes control characters and special characters for BigQuery queries
    val escape = (stringLit: String) => SqlUtils.escapeString(stringLit)

    // A mapping from user column id to physical column name (as stored in BigQuery)
    val userToPhysicalColumnMapping = bqUtils.getUserToSystemColumnMap(datasetInternalName).getOrElse {
      sys.error("Could not obtain systemToUserColumnMap")
    }
    val bqReps = generateReps(analysis)
    val qrySchema = querySchema(analysis)
    val bqRowReader = new BBQRowReader[SoQLType, SoQLValue]

    // Print the schema for this query
    logger.debug("Query schema: ")
    bqReps.foreach { case (k, v) => logger.debug(s"$k: ${v.repType}") }

    // We need a table name in the form: [dataset-id.table-name] to execute the query
    val bqTableName = bqUtils.makeFullTableIdentifier(config.bigqueryDatasetId, datasetInternalName, cinfo.copyNumber)

    // Execute the query and retrieve an iterator containing the results
    val results = managed(bqRowReader.query(
      analysis,
      (a: SoQLAnalysis[UserColumnId, SoQLType], tableName: String) =>
        new SoQLAnalysisSqlizer(a, tableName).sql(userToPhysicalColumnMapping, Seq.empty, sqlCtx, escape),
      rowCount,
      bqReps,
      new BBQQuerier(config.bigqueryProjectId),
      bqTableName))
    (qrySchema, results)
  }

  /**
   * @param analysis parsed soql
   * @return Use the query schema to create the appropriate BigQueryReps for the SoQLTypes
   *         associated with each column.
   */
  // TODO: Handle expressions and column aliases.
  private def generateReps(analysis: SoQLAnalysis[UserColumnId, SoQLType]):
                  OrderedMap[ColumnId, BBQReadRep[SoQLType, SoQLValue]] = {

    analysis.selection.foldLeft(OrderedMap.empty[ColumnId, BBQReadRep[SoQLType, SoQLValue]]) { (map, entry) =>
      entry match {
        case (columnName: ColumnName, coreExpr: CoreExpr[UserColumnId, SoQLType]) =>
          val cid = new ColumnId(map.size + 1)
          val bqRep = coreExpr match {
            case FunctionCall(function, _) if function.function.identity == "extent" => {
              new BoundingBoxRep
            }
            case otherExpr => BBQRepFactory(otherExpr.typ)
          }
          map + (cid -> bqRep)
      }
    }
  }

  /**
   * @param analysis parsed soql
   * @return a schema for the selected columns
   */
  // TODO: Handle expressions and column aliases.
  private def querySchema(analysis: SoQLAnalysis[UserColumnId, SoQLType]):
      OrderedMap[ColumnId, ColumnInfo[SoQLType]] = {

    analysis.selection.foldLeft(OrderedMap.empty[ColumnId, ColumnInfo[SoQLType]]) { (map, entry) =>
      entry match {
        case (columnName: ColumnName, coreExpr: CoreExpr[UserColumnId, SoQLType]) =>
          val cid = new ColumnId(map.size + 1)
          val cinfo = new ColumnInfo[SoQLType](
            null,
            cid,
            new UserColumnId(columnName.name),
            coreExpr.typ,
            columnName.name,
            coreExpr.typ == SoQLID,
            false, // isUserKey
            coreExpr.typ == SoQLVersion
          )(SoQLTypeContext.typeNamespace, null)
          map + (cid -> cinfo)
      }
    }
  }

  // Returns the schema for the /schema API endpoint
  private def getSchema(datasetName : String): Option[Schema] = {
    bqUtils.getSchema(datasetName)
  }

  private def copyInfoHeader(copyNumber: Long, dataVersion: Long, lastModified: DateTime) = {
    Header("Last-Modified", lastModified.toHttpDate) ~>
      Header("X-SODA2-CopyNumber", copyNumber.toString) ~>
      Header("X-SODA2-DataVersion", dataVersion.toString)
  }
}

object QueryServer extends DynamicPortMap with Logging {

  sealed abstract class QueryResult
  case class NotModified(etags: Seq[EntityTag]) extends QueryResult
  case object PreconditionFailed extends QueryResult
  case class Success(
    qrySchema: OrderedMap[ColumnId, ColumnInfo[SoQLType]],
    copyNumber: Long,
    dataVersion: Long,
    results: Managed[CloseableIterator[Row[SoQLValue]] with RowCount],
    etag: EntityTag,
    lastModified: DateTime
  ) extends QueryResult


  def withDefaultAddress(config: Config): Config = {
    val ifaces = ServiceInstanceBuilder.getAllLocalIPs
    if(ifaces.isEmpty) config
    else {
      val first = JString(ifaces.iterator.next().getHostAddress)
      val addressConfig = ConfigFactory.parseString("com.socrata.soql-server-bq.service-advertisement.address=" + first)
      config.withFallback(addressConfig)
    }
  }

  val config = try {
    new QueryServerConfig(withDefaultAddress(ConfigFactory.load()), "com.socrata.soql-server-bq")
  } catch {
    case e: Exception =>
      logger.error(s"$e")
      logger.error("config function broke")
      sys.exit(1)
  }

  PropertyConfigurator.configure(Propertizer("log4j", config.log4j))

  def main(args:Array[String]) {

    val address = config.discovery.address
    val datasourceConfig = new DataSourceConfig(config.getRawConfig("store"), "database")

    println(datasourceConfig)

    implicit object executorResource extends com.rojoma.simplearm.Resource[ExecutorService] {
      def close(a: ExecutorService) { a.shutdown() }
    }

    for {
      curator <- CuratorFromConfig(config.curator)
      discovery <- DiscoveryFromConfig(classOf[AuxiliaryData], curator, config.discovery)
      pong <- managed(new LivenessCheckResponder(config.livenessCheck))
      executor <- managed(Executors.newCachedThreadPool())
      dsInfo <- DataSourceFromConfig(datasourceConfig)
      conn <- managed(dsInfo.dataSource.getConnection)
      reporter <- MetricsReporter.managed(config.metrics)
    } {
      pong.start()
      val queryServer = new QueryServer(config, new BBQCommon(dsInfo, config.bigqueryProjectId), dsInfo, CaseSensitive)
      val advertisedLivenessCheckInfo = new LivenessCheckInfo(hostPort(pong.livenessCheckInfo.getPort),
                                                              pong.livenessCheckInfo.getResponse)
      val auxData = new AuxiliaryData(livenessCheckInfo = Some(advertisedLivenessCheckInfo))
      val curatorBroker = new CuratorBroker(discovery,
                                            address,
                                            config.discovery.name + "." + config.instance,
                                            Some(auxData)) {
        override def register(port: Int): Cookie = {
          super.register(hostPort(port))
        }
      }
      val logOptions = NewLoggingHandler.defaultOptions.copy(
                         logRequestHeaders = Set(ReqIdHeader, "X-Socrata-Resource"))
      val handler = ThreadRenamingHandler(NewLoggingHandler(logOptions)(queryServer.route))
      val server = new SocrataServerJetty(handler,
                        SocrataServerJetty.defaultOptions.
                       withPort(config.port).
                       withExtraHandlers(List(SocrataHttpSupport.getHandler(config.metrics))).
                       withPoolOptions(SocrataServerJetty.Pool(config.threadpool)).
                       withBroker(curatorBroker))
      logger.info("starting bbq query server")
      server.run()
    }
    logger.info("bbq query server exited")
  }
}
